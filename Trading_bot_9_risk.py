#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
업비트 자동매매 봇 v1.0 - Risk Management Module
========================================
책임: 통합 리스크 관리, 킬스위치 제어, 원자적 청산
- 연속 손실/실패 모니터링
- 일일 손실 한도 관리
- 킬스위치 작동 및 복구
- 원자적 긴급 청산 제어

제공 인터페이스:
  - check_risk_conditions(): 리스크 조건 체크
  - activate_kill_switch(): 킬스위치 작동
  - deactivate_kill_switch(): 킬스위치 해제
  - get_risk_status(): 리스크 상태 조회
  - should_reduce_position(): 포지션 축소 배수
  
금지사항:
  - 직접적인 주문 실행 (bot 모듈에서 처리)
  - 직접적인 포지션 청산 (bot 모듈에서 처리)
  - 성과 계산 (performance 모듈에서 처리)
  - 시장 분석 (analysis 모듈에서 처리)
  
데이터 흐름:
  Performance → Risk → Bot (킬스위치 신호)
  
개선사항:
  - 과도한 재시도 로직 제거
  - 백업 메커니즘 단순화
  - 불필요한 로그 제거
========================================
"""

import asyncio
import json
import sqlite3
import uuid
from datetime import datetime, timedelta
from typing import Dict, Optional, List
from collections import deque
from enum import Enum
from dataclasses import dataclass, field

from Trading_bot_1_core import logger, singleton
from global_constants import *

class RiskLevel(Enum):
    """리스크 수준"""
    NORMAL = "normal"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"
    SHUTDOWN = "shutdown"

class KillSwitchReason(Enum):
    """킬스위치 작동 사유"""
    CONSECUTIVE_LOSSES = "연속손실초과"
    DAILY_LOSS_LIMIT = "일일손실한도"
    CONSECUTIVE_ERRORS = "연속오류발생"
    DRAWDOWN_LIMIT = "최대낙폭초과"
    MANUAL_STOP = "수동중지"

@dataclass
class KillSwitchEvent:
    """킬스위치 이벤트"""
    event_id: str
    timestamp: datetime
    reason: KillSwitchReason
    positions_to_clear: List[str]
    cleared_positions: List[str] = field(default_factory=list)
    failed_positions: List[str] = field(default_factory=list)
    status: str = "PENDING"
    checkpoints: Dict = field(default_factory=dict)
    
    def to_dict(self) -> Dict:
        return {
            'event_id': self.event_id,
            'timestamp': self.timestamp.isoformat(),
            'reason': self.reason.value,
            'positions_to_clear': self.positions_to_clear,
            'cleared_positions': self.cleared_positions,
            'failed_positions': self.failed_positions,
            'status': self.status,
            'checkpoints': self.checkpoints
        }
    
    @classmethod
    def from_dict(cls, data: Dict):
        return cls(
            event_id=data['event_id'],
            timestamp=datetime.fromisoformat(data['timestamp']),
            reason=KillSwitchReason(data['reason']),
            positions_to_clear=data['positions_to_clear'],
            cleared_positions=data.get('cleared_positions', []),
            failed_positions=data.get('failed_positions', []),
            status=data.get('status', 'PENDING'),
            checkpoints=data.get('checkpoints', {})
        )

@singleton
class RiskManager:
    """
    통합 리스크 관리자 - 싱글톤
    책임: 리스크 모니터링 및 킬스위치 제어
    """
    # 🆕 체제별 Profit Factor 임계값, 킬 스위치 로직에만 사용함
    REGIME_PF_THRESHOLDS = {
        'TRENDING_UP': 1.0,      # 2.0 → 1.2 → 0.7
        'TRENDING_DOWN': 1.0,    # 1.5 → 1.2 → 1.5
        'BREAKOUT': 1.0,         # 1.8 → 1.2 → 0.8
        'RANGING': 1.0,          # 1.5 → 1.2 → 0.7
        'BREAKDOWN': 1.0,        # 1.3 → 1.2 → 1.3
        'UNKNOWN': 1.0           # 1.5 → 1.2 → 0.8
    }

    def __init__(self, config=None):
        self.config = config
        self.is_active = True
        self.kill_switch_active = False
        self.risk_level = RiskLevel.NORMAL
        
        # 리스크 임계값
        if config:
            self.max_consecutive_losses = config.get_cached('max_consecutive_losses') or 5
            self.critical_losses = self.max_consecutive_losses + 2
            
            # ========================================
            # 🆕 일일 손실 한도 (비율 + 고정 병행)
            # ========================================
            # 비율 기반 (우선)
            self.daily_max_loss_pct = config.get_cached('daily_max_loss_pct')
            
            # 고정 금액 (하위 호환)
            self.daily_max_loss = config.get_cached('daily_max_loss')
            
            # 기본값 설정 (둘 다 없으면)
            if self.daily_max_loss_pct is None and self.daily_max_loss is None:
                self.daily_max_loss_pct = 0.02  # 기본 2%
                logger.warning("⚠️ 일일 손실 한도 미설정 - 기본값 2% 사용")
            
            self.max_drawdown = config.get_cached('max_drawdown_pct') or 0.15
            self.max_errors = config.get_cached('max_consecutive_errors') or 10
            self.recovery_hours = config.get_cached('recovery_wait_hours') or 2
            self.auto_resume = config.get_cached('auto_resume_enabled') or False
        else:
            self.max_consecutive_losses = 5
            self.critical_losses = 7
            self.daily_max_loss_pct = 0.02  # 🆕 기본 2%
            self.daily_max_loss = None
            self.max_drawdown = 0.15
            self.max_errors = 10
            self.recovery_hours = 2
            self.auto_resume = False

        # ========================================
        # 🆕 로그 출력 (비율/고정 구분)
        # ========================================
        logger.info(
            f"[리스크관리] 연속손실 한계: "
            f"WARNING {int(self.max_consecutive_losses * 0.6)}회 "
            f"/ CRITICAL {self.max_consecutive_losses}회 "
            f"/ SHUTDOWN {self.critical_losses}회"
        )
        
        if self.daily_max_loss_pct:
            logger.info(
                f"[리스크관리] 일일손실한도: 잔고의 {self.daily_max_loss_pct*100:.1f}% (동적)"
            )
        elif self.daily_max_loss:
            logger.info(
                f"[리스크관리] 일일손실한도: {self.daily_max_loss:,.0f}원 (고정)"
            )
        
        if config:
            excluded_str = config.get('EXCLUSION_LIST', 'excluded_markets', fallback='')
            self.excluded_markets = set(
                m.strip() for m in excluded_str.replace(' ', '').split(',') if m.strip()
            )
            if self.excluded_markets:
                logger.info(f"[Risk] 제외 종목: {', '.join(sorted(self.excluded_markets))}")
        else:
            self.excluded_markets = set()
            
        # 모니터링 카운터
        self.consecutive_losses = 0
        self.consecutive_errors = 0
        self.daily_net_pnl = 0.0  # 🆕 순손익 추가
        self.daily_loss = 0.0      # 호환성 유지
        self.daily_reset_time = datetime.now().replace(hour=0, minute=0, second=0)
        
        # 이벤트 기록
        self.risk_events = deque(maxlen=MEMORY_BOUNDS['error_history'])
        self.kill_switch_history = deque(maxlen=10)
        
        # 킬스위치 이벤트
        self.active_kill_event: Optional[KillSwitchEvent] = None
        
        # 복구 상태
        self.recovery_mode = False
        self.recovery_start = None
        
        # 락
        self._lock = asyncio.Lock()
        
        # 🆕 DB 연결 풀
        self._conn = None
        self._conn_lock = asyncio.Lock()
        
        # DB 초기화
        self._init_db()
        self._load_pending_events()
    
    def _get_connection(self):
        """DB 연결 재사용
        Returns:
            sqlite3.Connection: 재사용 가능한 DB 연결
        """
        if self._conn is None:
            self._conn = sqlite3.connect(
                self.db_path,
                timeout=SQLITE_TIMEOUT,
                check_same_thread=False
            )
            logger.debug("✅ 킬스위치 DB 연결 생성")
        
        return self._conn
    
    def _init_db(self):
        """킬스위치 이벤트 DB 초기화"""
        self.db_path = "kill_switch_events.db"
        try:
            conn = self._get_connection()  # ✅ 연결 재사용
            
            conn.execute("""
            CREATE TABLE IF NOT EXISTS kill_events (
                event_id TEXT PRIMARY KEY,
                timestamp TEXT NOT NULL,
                reason TEXT NOT NULL,
                status TEXT NOT NULL,
                data TEXT NOT NULL
            )""")
            
            conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON kill_events(status)")
            conn.commit()
            
            logger.debug("킬스위치 DB 초기화 완료")
            
        except Exception as e:
            logger.error(f"킬스위치 DB 초기화 실패: {e}")
    
    def _load_pending_events(self):
        """미완료 킬스위치 이벤트 로드 - 자동 정리"""
        try:
            conn = self._get_connection()  # ✅ 연결 재사용
            
            now_time = datetime.now().isoformat()
            
            cursor = conn.execute("""
                SELECT COUNT(*) FROM kill_events 
                WHERE status IN ('PENDING', 'IN_PROGRESS')
            """)
            
            pending_count = cursor.fetchone()[0]
            
            if pending_count > 0:
                conn.execute("""
                    UPDATE kill_events 
                    SET status = 'COMPLETED' 
                    WHERE status IN ('PENDING', 'IN_PROGRESS')
                """)
                conn.commit()
                logger.info(f"🧹 프로그램 시작 - 미완료 킬스위치 {pending_count}개 자동 정리 완료")
            else:
                logger.debug("✅ 미완료 킬스위치 이벤트 없음")
            
            # ❌ conn.close() 제거
            
            # 킬스위치 상태 강제 초기화
            self.kill_switch_active = False
            self.active_kill_event = None
            logger.info("✅ 킬스위치 상태 초기화 완료 - 정상 거래 준비")
            
        except Exception as e:
            logger.error(f"킬스위치 이벤트 로드 실패: {e}")
            self.kill_switch_active = False
            self.active_kill_event = None
    
    async def check_risk_conditions(self, metrics: Dict) -> bool:
        """리스크 조건 체크 - 통합 손익비 기반
        
        3대 조건:
        1. 연속손실 (Profit Factor 고려)
        2. 일일 금액 한도 (순손익 기준)
        3. 최대 낙폭 (Profit Factor 고려)
        """
        async with self._lock:
            if not self.is_active:
                logger.debug("🔄 리스크 체크 스킵: 킬스위치 발동 중")
                return True  # 🔥 True 반환 (거래 계속)
            
            # 일일 리셋
            await self._check_daily_reset()
            
            # ========================================
            # 손익비 계산 (공통)
            # ========================================
            daily_profit = metrics.get('daily_profit_sum', 0)
            daily_loss = abs(metrics.get('daily_loss_amount', 0))
            daily_net_pnl = metrics.get('daily_net_pnl', 0)
            
            if daily_loss > 0 and daily_profit > 0:
                profit_factor = daily_profit / daily_loss
                loss_ratio = daily_loss / daily_profit
            else:
                profit_factor = float('inf') if daily_profit > 0 else 0
                loss_ratio = 0.0 if daily_profit > 0 else 1.0
            
            # ========================================
            # 로그: 현재 상태
            # ========================================
            logger.info("=" * 60)
            logger.info("🔍 [리스크 체크] 현재 상태:")
            logger.info(f"   • 수익: +{daily_profit:,.0f}원")
            logger.info(f"   • 손실: -{daily_loss:,.0f}원")
            logger.info(f"   • 순손익: {daily_net_pnl:+,.0f}원")
            logger.info(f"   • Profit Factor: {profit_factor:.2f}")
            logger.info(f"   • 연속손실: {metrics.get('consecutive_losses', 0)}회")
            logger.info(f"   • 낙폭: {abs(metrics.get('max_drawdown', 0))*100:.2f}%")
            logger.info("=" * 60)
            
            # 메트릭 업데이트
            self.consecutive_losses = metrics.get('consecutive_losses', 0)
            self.consecutive_errors = metrics.get('consecutive_errors', 0)
            self.daily_net_pnl = daily_net_pnl
            current_balance = metrics.get('current_balance', 10000000)
            
            # 일일 한도 계산 (✅ abs 추가 - 잔고가 음수여도 한도는 양수)
            if self.daily_max_loss_pct:
                daily_limit = abs(current_balance) * self.daily_max_loss_pct
            elif self.daily_max_loss:
                daily_limit = abs(self.daily_max_loss)
            else:
                daily_limit = abs(current_balance) * 0.02

            logger.debug(f"[일일한도] {daily_limit:,.0f}원")
            
            # ========================================
            # 킬스위치 조건 체크 (단순화 버전)
            # ========================================
            kill_reasons = []

            # 1️⃣ 연속손실 (단순 횟수만 체크) - 2025.10.22 단순화
            if self.consecutive_losses >= 20:  # 20회로 상향
                logger.critical(
                    f"❌ 연속손실 한계: {self.consecutive_losses}회 >= 20회"
                )
                kill_reasons.append(KillSwitchReason.CONSECUTIVE_LOSSES)
            else:
                logger.debug(
                    f"✅ 연속손실 정상: {self.consecutive_losses}회"
                )

            # 2️⃣ 일일 금액 한도 (순손익 기준)
            if daily_net_pnl < 0 and abs(daily_net_pnl) >= daily_limit:
                # ✅ ZeroDivision 방어
                loss_pct = (abs(daily_net_pnl) / abs(current_balance) * 100) if current_balance != 0 else 0
                logger.critical(
                    f"❌ 일일손실한도 초과: {abs(daily_net_pnl):,.0f}원 >= "
                    f"{daily_limit:,.0f}원 ({loss_pct:.2f}%)"
                )
                kill_reasons.append(KillSwitchReason.DAILY_LOSS_LIMIT)
            else:
                if daily_net_pnl >= 0:
                    logger.debug(
                        f"✅ 일일손익 정상: +{daily_net_pnl:,.0f}원 "
                        f"(PF: {profit_factor:.2f})"
                    )
                else:
                    logger.debug(
                        f"✅ 일일손익 정상: {abs(daily_net_pnl):,.0f}원 < "
                        f"한도 {daily_limit:,.0f}원"
                    )

            # 3️⃣ 최대 낙폭 (단순 비율만) - 2025.10.22 단순화
            drawdown = abs(metrics.get('max_drawdown', 0))

            if drawdown >= self.max_drawdown:
                logger.critical(
                    f"❌ 최대낙폭 초과: {drawdown*100:.1f}% >= "
                    f"{self.max_drawdown*100:.1f}%"
                )
                kill_reasons.append(KillSwitchReason.DRAWDOWN_LIMIT)
            else:
                logger.debug(f"✅ 낙폭 정상: {drawdown*100:.1f}%")

            # 4️⃣ 연속오류
            if self.consecutive_errors >= self.max_errors:
                logger.critical(
                    f"❌ 연속오류 한도: {self.consecutive_errors}회 >= "
                    f"{self.max_errors}회"
                )
                kill_reasons.append(KillSwitchReason.CONSECUTIVE_ERRORS)
            else:
                logger.debug(
                    f"✅ 연속오류 정상: {self.consecutive_errors}회"
                )

            # 킬스위치 작동
            if kill_reasons:
                logger.critical("=" * 60)
                logger.critical(
                    f"🚨 킬스위치 발동 사유: "
                    f"{[r.value for r in kill_reasons]}"
                )
                logger.critical("=" * 60)
                await self.activate_kill_switch(kill_reasons[0])
                return False
            
            # 리스크 레벨 업데이트
            self._update_risk_level(metrics)
            
            return True
    
    def _update_risk_level(self, metrics: Dict):
        """리스크 레벨 업데이트 (Profit Factor 기반)"""
        old_level = self.risk_level
        
        # 손익비 계산
        daily_profit = metrics.get('daily_profit_sum', 0)
        daily_loss = abs(metrics.get('daily_loss_amount', 0))
        
        if daily_loss > 0 and daily_profit > 0:
            profit_factor = daily_profit / daily_loss
            loss_ratio = daily_loss / daily_profit
        else:
            profit_factor = float('inf')
            loss_ratio = 0.0
        
        # 🔥 수정: AND 조건으로 변경 (더 관대하게)
        if self.kill_switch_active:
            self.risk_level = RiskLevel.SHUTDOWN
        elif (self.consecutive_losses >= self.max_consecutive_losses and 
            profit_factor < 0.5):  # ← 기존: or loss_ratio > 0.9
            self.risk_level = RiskLevel.CRITICAL
        elif (self.consecutive_losses >= self.max_consecutive_losses * 0.6 and 
            profit_factor < 0.7):  # ← 기존: or loss_ratio > 0.8
            self.risk_level = RiskLevel.WARNING
        else:
            self.risk_level = RiskLevel.NORMAL
        
        # 레벨 변경 시 로그
        if old_level != self.risk_level:
            logger.info(f"리스크레벨 변경: {old_level.value} → {self.risk_level.value}")
            
            if self.risk_level == RiskLevel.WARNING:
                logger.warning(
                    f"⚠️ WARNING 레벨 진입\n"
                    f"   • 연속손실: {self.consecutive_losses}회 OR "
                    f"손익비: {profit_factor:.2f}\n"
                    f"   • 수익: +{daily_profit:,.0f}원 | "
                    f"손실: -{daily_loss:,.0f}원\n"
                    f"   • 영향: 포지션 사이징 50% 축소"
                )
            elif self.risk_level == RiskLevel.CRITICAL:
                logger.critical(
                    f"🚨 CRITICAL 레벨 진입\n"
                    f"   • 연속손실: {self.consecutive_losses}회 OR "
                    f"손익비: {profit_factor:.2f}\n"
                    f"   • 수익: +{daily_profit:,.0f}원 | "
                    f"손실: -{daily_loss:,.0f}원\n"
                    f"   • 포지션 사이징 30% 축소"
                )
            elif self.risk_level == RiskLevel.NORMAL and old_level != RiskLevel.NORMAL:
                logger.info(
                    f"✅ NORMAL 레벨 복귀 "
                    f"(PF: {profit_factor:.2f})"
                )
    
    async def activate_kill_switch(self, reason: KillSwitchReason) -> str:
        """킬스위치 작동"""
        if self.kill_switch_active and self.active_kill_event:
            return self.active_kill_event.event_id
        
        # 이벤트 생성
        event_id = f"KILL_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        
        self.active_kill_event = KillSwitchEvent(
            event_id=event_id,
            timestamp=datetime.now(),
            reason=reason,
            positions_to_clear=[]
        )
        
        self.kill_switch_active = True
        # self.is_active = False  # 🔥 삭제! 서비스는 계속
        self.risk_level = RiskLevel.SHUTDOWN
        
        # DB 저장
        await self._persist_kill_event(self.active_kill_event)
        
        # 기록
        self.kill_switch_history.append({
            'timestamp': datetime.now().isoformat(),
            'event_id': event_id,
            'reason': reason.value
        })
        
        logger.critical(
            f"🚨 킬스위치 작동: {reason.value} (ID: {event_id})\n"
            f"   ⚠️ 신규 매수 차단\n"
            f"   ✅ 기존 포지션 모니터링 계속\n"
            f"   ✅ 손절/익절 정상 작동"
        )
        
        # 자동 복구 설정
        if self.auto_resume:
            self.recovery_mode = True
            self.recovery_start = datetime.now()
        
        return event_id
    
    async def _persist_kill_event(self, event: KillSwitchEvent):
        """킬스위치 이벤트 저장 - 연결 재사용"""
        try:
            # JSON 백업
            backup_path = f"kill_switch_{event.event_id}.json"
            with open(backup_path, 'w') as f:
                json.dump(event.to_dict(), f, indent=2)
            
            # DB 저장
            async with self._conn_lock:  # 🆕 Lock
                conn = self._get_connection()  # ✅ 연결 재사용
                
                conn.execute("""
                INSERT OR REPLACE INTO kill_events (event_id, timestamp, reason, status, data)
                VALUES (?, ?, ?, ?, ?)
                """, (
                    event.event_id,
                    event.timestamp.isoformat(),
                    event.reason.value,
                    event.status,
                    json.dumps(event.to_dict())
                ))
                
                conn.commit()
                # ❌ conn.close() 제거
            
        except Exception as e:
            logger.error(f"킬스위치 저장 실패: {e}")
    
    def create_kill_event(self, positions: List[str]) -> KillSwitchEvent:
        """킬스위치 이벤트에 포지션 설정"""
        if self.active_kill_event:
            self.active_kill_event.positions_to_clear = positions
            return self.active_kill_event
        raise ValueError("활성 킬스위치 이벤트 없음")
    
    def get_kill_event(self, event_id: str) -> Optional[KillSwitchEvent]:
        """킬스위치 이벤트 조회"""
        if self.active_kill_event and self.active_kill_event.event_id == event_id:
            return self.active_kill_event
        return None
    
    async def update_kill_event(self, event: KillSwitchEvent):
        """킬스위치 이벤트 업데이트"""
        if self.active_kill_event and self.active_kill_event.event_id == event.event_id:
            self.active_kill_event = event
        
        await self._persist_kill_event(event)
        
        if event.status in ["COMPLETED", "FAILED"]:
            if event.status == "COMPLETED":
                await self.deactivate_kill_switch()
    
    async def deactivate_kill_switch(self, manual: bool = False):
        """킬스위치 해제"""
        if not self.kill_switch_active:
            return
        
        self.kill_switch_active = False
        self.is_active = True
        self.recovery_mode = False
        self.recovery_start = None
        self.active_kill_event = None
        
        self.consecutive_losses = 0
        self.consecutive_errors = 0
        self.risk_level = RiskLevel.NORMAL
        
        logger.info(f"✅ 킬스위치 해제: {'수동' if manual else '자동복구'}")
    
    async def _check_daily_reset(self):
        """일일 리셋 체크"""
        now = datetime.now()
        next_reset = self.daily_reset_time + timedelta(days=1)

        if now >= next_reset:
            self.daily_net_pnl = 0
            self.daily_loss = 0
            self.daily_reset_time = now.replace(hour=0, minute=0, second=0)

            # 새날에는 일일손실 사유의 킬스위치만 종료한다.
            if (
                self.kill_switch_active
                and self.active_kill_event is not None
                and self.active_kill_event.reason
                == KillSwitchReason.DAILY_LOSS_LIMIT
            ):
                self.active_kill_event.status = "COMPLETED"
                await self.update_kill_event(self.active_kill_event)
                logger.info(
                    "✅ 일일 리셋: 일일손실 킬스위치 자동 해제"
                )

            # 다른 사유의 기존 시간 기반 자동 복구는 유지한다.
            if self.recovery_mode and self.recovery_start:
                elapsed = (
                    now - self.recovery_start
                ).total_seconds() / 3600
                if elapsed >= self.recovery_hours:
                    await self.deactivate_kill_switch()
    
    def get_fastentry_multiplier(self, *, 
                                 confidence: float,
                                 adx: float = 0.0,
                                 volume_ratio: float = 1.0,
                                 ma_bull_aligned: bool = False,
                                 signal_age_sec: int = 9999) -> float:
        """
        FastEntry 조건 충족 시 포지션 사이징 가중치(>1.0)를 반환.
        조건 미충족·보수상황이면 1.0 이하(=축소)도 가능.        
        Args:
            confidence: 신호 신뢰도 (0~1)
            adx: ADX 추세 강도 (0~100)
            volume_ratio: 거래량 배수 (1.0=평균)
            ma_bull_aligned: MA 정배열 여부 (단기>중기)
            signal_age_sec: 신호 생성 후 경과 시간(초)            
        Returns:
            포지션 배수 (0.5~1.6, 기본 1.0)
        """
        try:
            if not self.config.getboolean('RISK_MANAGEMENT', 'fastentry_enabled', fallback=True):
                return 1.0
            
            # 설정값 로드
            floor_c = self.config.getfloat('RISK_MANAGEMENT', 'fastentry_confidence_floor', fallback=0.62)
            adx_min = self.config.getfloat('RISK_MANAGEMENT', 'fastentry_adx_min', fallback=22)
            vr_min  = self.config.getfloat('RISK_MANAGEMENT', 'fastentry_volratio_min', fallback=1.10)
            need_ma = self.config.getboolean('RISK_MANAGEMENT', 'fastentry_ma_alignment_needed', fallback=True)
            tb_sec  = self.config.getint('RISK_MANAGEMENT', 'fastentry_timebox_sec', fallback=180)
            mult_max= self.config.getfloat('RISK_MANAGEMENT', 'fastentry_risk_mult_max', fallback=1.6)
            step    = self.config.getfloat('RISK_MANAGEMENT', 'fastentry_risk_mult_step', fallback=0.25)

            # 기본값
            mult = 1.0

            # FastEntry 조건 체크
            if (confidence >= floor_c and 
                adx >= adx_min and 
                volume_ratio >= vr_min and 
                signal_age_sec <= tb_sec):
                
                # MA 정배열 체크 (필요시)
                if (not need_ma) or ma_bull_aligned:
                    # 단계별 가중치: 신뢰도/ADX/거래량 기준
                    bumps = 0
                    if confidence >= (floor_c + 0.05): bumps += 1  # +5%p 이상
                    if adx >= (adx_min + 8): bumps += 1            # +8p 이상
                    if volume_ratio >= (vr_min + 0.25): bumps += 1 # +0.25배 이상
                    
                    mult = min(mult_max, 1.0 + step * (1 + bumps))
                    logger.debug(
                        f"[FastEntry] 확대×{mult:.2f} | "
                        f"신뢰도={confidence:.2f} ADX={adx:.1f} "
                        f"거래량={volume_ratio:.2f}배 bumps={bumps}"
                    )
            else:
                # 리스크 축소 조건 (연속손실)
                if self.consecutive_losses >= 5:
                    mult = 0.8
                    logger.debug(f"[FastEntry] 축소×{mult:.2f} | 연속손실={self.consecutive_losses}회")

            return max(0.5, mult)  # 하한 0.5배
            
        except Exception as e:
            logger.warning(f"[Risk] FastEntry multiplier 계산 오류: {e}")
            return 1.0
    
    def should_reduce_position(self) -> float:
        """리스크 레벨에 따른 포지션 축소 배수"""
        if self.risk_level == RiskLevel.CRITICAL:
            return 0.3
        elif self.risk_level == RiskLevel.WARNING:
            return 0.5
        elif self.risk_level == RiskLevel.NORMAL:
            return 1.0
        else:
            return 0.0
    
    @property
    def current_level(self) -> str:
        """현재 리스크 레벨 (문자열)"""
        return self.risk_level.value
    
    def get_risk_status(self) -> Dict:
        """리스크 상태 조회"""
        return {
            'is_active': self.is_active,
            'kill_switch': self.kill_switch_active,
            'level': self.risk_level.value,
            'consecutive_losses': self.consecutive_losses,
            'daily_loss': self.daily_loss,
            'consecutive_errors': self.consecutive_errors,
            'position_multiplier': self.should_reduce_position(),
            'recovery_mode': self.recovery_mode,
            'active_kill_event': self.active_kill_event.event_id if self.active_kill_event else None
        }
    
    def export_report(self) -> Dict:
        """리스크 리포트 생성"""
        return {
            'timestamp': datetime.now().isoformat(),
            'status': self.get_risk_status(),
            'thresholds': {
                'max_consecutive_losses': self.max_consecutive_losses,
                'critical_losses': self.critical_losses,
                'daily_max_loss': self.daily_max_loss,
                'max_drawdown': self.max_drawdown,
                'max_errors': self.max_errors
            },
            'events': list(self.risk_events)[-10:] if self.risk_events else [],
            'kill_switch_history': list(self.kill_switch_history),
            'active_kill_event': self.active_kill_event.to_dict() if self.active_kill_event else None
        }
        
    async def record_recovery_win(self, symbol: str, pnl: float, recovery_info: Dict = None):
        """회복 거래 성공 기록 및 레벨 조정
        Args:
            symbol: 거래 종목
            pnl: 수익 금액 (양수)
            recovery_info: 회복 거래 정보 (entry_time, confidence 등)
        Returns:
            bool: 모드 전환 성공 여부
        Effects:
            - 연속손실 카운터 리셋 확인
            - WARNING → NORMAL 레벨 전환
            - 캐시보존모드 해제 로그
        """
        if pnl <= 0:
            logger.warning(f"[회복거래] {symbol} 손실 발생 - 회복 실패 ({pnl:,.0f}원)")
            return False
        
        logger.info(
            f"🌟 [회복거래] {symbol} 성공! "
            f"수익: {pnl:,.0f}원"
        )
        
        # ========================================
        # 1. 현재 리스크 레벨 확인
        # ========================================
        if self.risk_level != RiskLevel.WARNING:
            logger.debug(
                f"[회복거래] 현재 레벨 {self.risk_level.value} - "
                f"WARNING이 아니므로 전환 불필요"
            )
            return False
        
        # ========================================
        # 2. 연속손실 상태 재계산
        # ========================================
        # Performance Manager에서 전체 연속손실 조회
        consecutive = 0
        
        if hasattr(self, 'performance') and self.performance:
            try:
                metrics = self.performance.get_risk_metrics()
                consecutive = metrics.get('consecutive_losses', 0)
            except:
                pass
        
        logger.debug(f"[회복거래] 전체 연속손실: {consecutive}회")
        
        # ========================================
        # 3. WARNING 해제 조건 확인
        # ========================================
        # WARNING 발동 조건:
        # - 연속손실 3회 이상 (max 5의 60%)
        # - 일일손실 80% 이상
        
        # 연속손실이 3 미만으로 회복되면 해제
        if consecutive < 3:
            logger.info(
                f"✅ [리스크관리] WARNING → NORMAL 전환! "
                f"연속손실: {consecutive}회 < 3회 (회복 기준)"
            )
            
            # 레벨 다운그레이드
            self.risk_level = RiskLevel.NORMAL
            
            # 이벤트 로그 (옵션)
            if hasattr(self, 'kill_switch_events'):
                self.kill_switch_events.append({
                    'timestamp': datetime.now(),
                    'event_type': 'RECOVERY',
                    'symbol': symbol,
                    'pnl': pnl,
                    'recovery_info': recovery_info,
                    'message': f'회복 거래 성공으로 WARNING 해제 (연속손실 {consecutive}회)'
                })
            
            return True
        else:
            logger.info(
                f"⚠️ [리스크관리] 회복 거래 성공했으나 WARNING 유지 "
                f"(연속손실: {consecutive}회 ≥ 3회)"
            )
            return False
        
    async def shutdown(self):
        """종료 처리 - DB 연결 정리"""
        try:
            logger.info("[리스크관리] 종료 처리 시작")
            
            # 🆕 DB 연결 종료
            if self._conn:
                try:
                    self._conn.close()
                    self._conn = None
                    logger.info("✅ 킬스위치 DB 연결 종료")
                except Exception as e:
                    logger.error(f"킬스위치 DB 종료 오류: {e}")
            
            logger.info("[리스크관리] 종료 처리 완료")
            
        except Exception as e:
            logger.error(f"[리스크관리] 종료 처리 오류: {e}")

# **END**           