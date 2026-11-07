#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
============================================================
⚠️ global_constants.py 역할 정의 (중요)
============================================================
이 파일은 다음 목적만을 가진다.
1. 기본 상수 정의 (폴백, 문서화, 초기 설계값)
2. ConfigManager 초기 병합 시 fallback 제공
3. Enum / 공통 타입 / 시스템 전역 상수

❌ 이 파일의 값은
   '실제 매매 판단의 최종 기준(SSoT)'이 아니다.

✔ 실제 매매 로직에서 사용되는 값은
   반드시 ConfigManager.get_cached()
   또는 TradingSettings.from_config()를 통해 주입된다.

⚠️ 주의:
- Strategy / Risk / Execution에서
  global_constants의 값을 직접 참조하지 말 것.
- 직접 참조는 설정 충돌, 디버깅 혼란,
  과거 이슈 재발의 원인이 된다.
============================================================
업비트 자동매매 봇 v1.0 - Global Constants Module
전역 상수 정의 - 모든 모듈에서 import하여 사용
모든 하드코딩된 숫자 리터럴의 중앙 관리
개선사항: 중복 제거 및 카테고리 정리
"""

# ================== 1. 봇 정보 ==================
BOT_VERSION = "v1.0"  # 봇 버전 정보
BOT_NAME = "업비트 자동매매 봇"  # 봇 이름

# ================== 2. 프로그램 종료 코드 ==================
EXIT_CODE_SUCCESS = 0  # 정상 종료
EXIT_CODE_FAILURE = 1  # 오류로 인한 종료
EXIT_CODE_CANCELLED = 2  # 사용자 취소로 인한 종료

# ================== 3. 배열/리스트 인덱싱 ==================
LAST_INDEX = -1  # 마지막 요소 접근용 인덱스
SECOND_LAST_INDEX = -2  # 뒤에서 두 번째 요소 접근용 인덱스
FIRST_INDEX = 0  # 첫 번째 요소 접근용 인덱스

# ================== 4. 샘플/윈도우 크기 ==================
RECENT_SAMPLE_COUNT = 5  # 최근 데이터 샘플 수 (단기 분석용)
RECENT_SAMPLE_WINDOW = 10  # 최근 데이터 윈도우 크기 (중기 분석용)
DEFAULT_ANALYSIS_WINDOW = 10  # 기본 분석 윈도우 크기
DEFAULT_HISTORY_WINDOW = 50  # 기본 히스토리 윈도우 크기 (장기 분석용)

# ================== 5. 수학/계산 관련 ==================
EPSILON = 1e-10  # 부동소수점 연산시 0 판단용 극소값
DEFAULT_SCORE = 0.5  # 기본 점수 (중립값)
DEFAULT_CONFIDENCE = 0.7  # 기본 신뢰도 (70%)
DEFAULT_MULTIPLIER = 1.0  # 기본 배수 (변화 없음)
MAJORITY_THRESHOLD = 0.5  # 과반수 판단 임계값 (50%)
NORMALIZATION_TOLERANCE = 0.001  # 정규화 허용 오차
ZERO_THRESHOLD = 0.0  # 영(0) 임계값
PERCENT_CONVERSION = 100.0  # 퍼센트 변환 계수 (소수 → 퍼센트)
PERCENT_TO_RATIO = 0.01  # 퍼센트를 비율로 변환 (1% = 0.01)

# ================== 6. 업비트 거래소 ==================
MIN_ORDER_AMOUNT = 5000  # 최소 주문 금액 (원)
UPBIT_MAKER_FEE = 0.0005  # 메이커 수수료 (0.05%)
UPBIT_TAKER_FEE = 0.0005  # 테이커 수수료 (0.05%)
MAX_CANDLES_PER_REQUEST = 200  # API 요청당 최대 캔들 수
BASIC_SLIPPAGE = 0.001  # 기본 슬리피지 (0.1%)

# ================== 7. HTTP 상태 코드 ==================
HTTP_SUCCESS = 200  # 성공
HTTP_TOO_MANY_REQUESTS = 429  # 요청 한도 초과
HTTP_SERVER_ERRORS = [500, 502, 503, 504]  # 서버 오류 코드 목록

# ================== 8. 시간 관련 ==================
SECONDS_PER_MINUTE = 60  # 1분 = 60초
MINUTES_PER_HOUR = 60  # 1시간 = 60분
HOURS_PER_DAY = 24  # 1일 = 24시간
HOURS_TO_SECONDS = 3600  # 1시간 = 3600초
DEFAULT_CACHE_TTL_MINUTES = 5  # 기본 캐시 유효시간 (분)

# ================== 9. 데이터 처리 ==================
DEFAULT_BATCH_SIZE = 20  # 기본 배치 크기
DEFAULT_BATCH_DELAY = 0.2  # 배치 처리 지연시간 (초)
DEFAULT_MAX_HISTORY_SIZE = 100  # 최대 히스토리 크기
MIN_SAMPLE_SIZE = 5  # 최소 샘플 크기
MAX_HISTORY_LIMIT = 100  # 히스토리 한계치
OPTIMAL_BATCH_SIZE = 10  # 최적 배치 크기
OPTIMAL_CONCURRENT_LIMIT = 15  # 최적 동시 처리 한계

# ================== 10. 임계값 및 기본값 ==================
DEFAULT_VOLATILITY_VALUE = 0.1  # 기본 변동성 값 (10%)
DEFAULT_ATR_PERIOD = 14  # ATR 계산 기간 (14캔들)
CONFIDENCE_PENALTY = 0.5  # 신뢰도 페널티 (50% 감소)
SAFETY_MODE_PENALTY = 0.5  # 안전모드 페널티 (50% 감소)
DEFAULT_TIMEOUT_CONFIDENCE = 0.7  # 타임아웃시 기본 신뢰도
DEFAULT_RELIABILITY_LOW = 0.35  # 낮은 신뢰도 기준값
MAX_ATR_NOISE = 0.75  # 최대 ATR 노이즈 비율
DEFAULT_TIMEFRAME = '5m'  # 기본 타임프레임
FALLBACK_TIMEFRAME = '5m'  # 폴백용 타임프레임

# ================== 11. 데이터 정밀도 ==================
PRICE_PRECISION = 0  # 가격 소수점 자릿수 (정수)
VOLUME_PRECISION = 8  # 거래량 소수점 자릿수
PERCENT_PRECISION = 1  # 퍼센트 소수점 자릿수

# ================== 12. 거래 메트릭 ==================
TRADING_DAYS_PER_YEAR = 365  # 연간 거래일 수 (코인 시장은 365일 24시간)
MIN_SAMPLES_FOR_SHARPE = 10  # Sharpe Ratio 계산 최소 샘플
MIN_SAMPLES_FOR_CALMAR = 20  # Calmar Ratio 계산 최소 샘플
CONFIDENCE_MIN_SAMPLES = 20  # 신뢰도 계산 최소 샘플

# ================== 13. 체제 전환 ==================
REGIME_STABILIZATION_SECONDS = 600  # 체제 안정화 시간 (10분)
REGIME_TRANSITION_THRESHOLD = 0.65  # 체제 전환 임계값 (65%)
REGIME_BREAKOUT_THRESHOLD = 0.995  # 돌파 체제 임계값 (99.5%)
REGIME_BREAKDOWN_THRESHOLD = 1.005  # 붕괴 체제 임계값 (100.5%)
REGIME_BREAKOUT_CONFIDENCE = 0.7  # 돌파 체제 신뢰도
REGIME_RANGING_CONFIDENCE = 0.6  # 횡보 체제 신뢰도

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🆕 PATCH: 타임프레임별 체제 안정화 시간 (초)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 원칙: 타임프레임 길이에 비례하는 안정화 기간
# - 짧은 타임프레임: 빠른 반응 but 최소 쿨다운 유지
# - 긴 타임프레임: 추세 지속성 확인 위한 긴 쿨다운
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
REGIME_STABILIZATION_BY_TIMEFRAME = {
    "1m": 60,     # ✅ 1분 (1캔들) - 플리핑 방지 최소 쿨다운
    "3m": 180,    # ✅ 3분 (1캔들) - 900 → 180 단축
    "5m": 300,    # ✅ 5분 (1캔들) - 1500 → 300 단축
    "10m": 1800,  # 30분 (3캔들) - 유지
    "15m": 2700,  # 45분 (3캔들) - 유지
    "30m": 5400,  # 90분 (3캔들) - 유지
    "1h": 10800,  # 3시간 (3캔들) - 유지
    "4h": 21600,  # 6시간 (1.5캔들) - 유지
    "1d": 43200,  # 12시간 (0.5캔들) - 유지
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🆕 체제 히스테리시스 임계값 (진입/탈출 비대칭)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# - entry: 새로운 체제로 "진입"하려면 필요한 최소 점수
# - exit : 현재 체제에서 "탈출"하려면 점수가 이 값 아래로 떨어져야 함
# - entry > exit 이므로 안정적인 체제 유지(Hysteresis)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
REGIME_HYSTERESIS_THRESHOLDS = {
    "TRENDING_UP":   {"entry": 4.0, "exit": 2.0},
    "TRENDING_DOWN": {"entry": 3.5, "exit": 1.5},
    "BREAKOUT":      {"entry": 4.5, "exit": 2.5},
    "BREAKDOWN":     {"entry": 3.5, "exit": 1.5},
    "RANGING":       {"entry": 2.0, "exit": 1.0},
}

# ================== 14. 리스크 관리 ==================
DEFAULT_KELLY_FRACTION = 0.3  # Kelly 비율 조정 계수 (30%)
DEFAULT_KELLY_MAX_WEIGHT = 0.6  # Kelly 최대 가중치 (60%)
DEFAULT_ADAPTATION_SPEED = 0.7  # 적응 속도 (70%)
MIN_SAMPLES_MAIN_SIGNALS = 15  # 주요 신호 최소 샘플
CONSECUTIVE_LOSS_THRESHOLD = 3  # 연속 손실 임계값 (3회)
CONSECUTIVE_LOSS_MULTIPLIER = 0.7  # 연속 손실시 포지션 축소 (70%)
WIN_STREAK_THRESHOLD = 3  # 연승 임계값 (3회)
WIN_STREAK_MULTIPLIER = 1.2  # 연승시 포지션 확대 (120%)

# ======================================================================
# Knee Mode / 저점대비 관련 전역 상수
# ======================================================================

# 저점대비 하한 (너무 깊게 빠져 있으면 페널티)
KNEE_LOW_MIN_PCT = -0.5  # -0.5%

# 체제별 저점대비 상한 (LOW_MAX)
KNEE_LOW_MAX_TRENDING_UP   = 2.0  # 🔥 상승장: 무릎 상한 강화 (3.0→2.0, XPL 어깨 진입 차단)
KNEE_LOW_MAX_TRENDING_DOWN = 3.0  # 하락장 반등: 3% 내에서만 허용 (추격 억제) 
KNEE_LOW_MAX_BREAKOUT      = 3.3  # 돌파장: 모멘텀 기반 상승 초기 여유(종전 3.0%, 2.5%)
KNEE_LOW_MAX_RANGING       = 2.2  # 횡보장: 추격·노이즈 방지를 위해 가장 타이트하게(종전 1.0%, 1.8%)
KNEE_LOW_MAX_DEFAULT       = 3.0  # 기본값: 안전한 추세·반등 포착 (종전 1.0%, 1.5%)

# 저점 탐색 구간 (최근 N봉 저점 기준)
KNEE_LOOKBACK_BARS = 80  # 3m × 80 = 240분 (4시간, 구조적 저점 포착)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🆕 체제별 필터 프로파일 (Market Regime-specific Filter Profiles)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
REGIME_FILTER_PROFILES = {
    "BULL_MARKET": {
        # 철학: 상승장에서 계단 반복 매매 허용 (복리 폭발)
        # 박스 무시, 모든 조건 완화, 무한 반복 스캘핑
        "ma5_max_dev": 2.5,          # MA5 상단 허용폭 크게
        "ma5_min_dev": -1.5,         # 하단도 완화
        "low_max_tier1": 4.0,        # 저점대비 상한 크게 (계단 여러 개)
        "low_max_tier2": 5.0,
        "low_max_tier3": 6.5,
        "box_hardcut": False,        # 박스 HardCut 완전 비활성화
        "box_threshold": 999,        # 사실상 무의미
        "early_entry_exception": True,
        "volume_boost": True,
        "volume_boost_threshold": 1.5,
        "volume_boost_multiplier": 2.0
    },

    "BREAKOUT": {
        "ma5_max_dev": 3.5,
        "ma5_min_dev": -1.0,
        "low_max_tier1": 6.0,
        "low_max_tier2": 7.5,
        "low_max_tier3": 10.0,
        "box_hardcut": False,
        "box_threshold": 999,
        "early_entry_exception": True,
        "volume_boost": True,
        "volume_boost_threshold": 3.0,
        "volume_boost_multiplier": 1.5
    },
    
    "TRENDING_UP": {
        "ma5_max_dev": 1.5,
        "ma5_min_dev": -1.0,
        "low_max_tier1": 2.5,
        "low_max_tier2": 3.0,
        "low_max_tier3": 4.0,
        "box_hardcut": False,
        "box_threshold": 85,
        "early_entry_exception": True,
        "volume_boost": False
    },
    
    "TRENDING_DOWN": {
        "ma5_max_dev": 0.5,
        "ma5_min_dev": -1.5,
        "low_max_tier1": 2.0,
        "low_max_tier2": 2.5,
        "low_max_tier3": 3.5,
        "box_hardcut": True,
        "box_threshold": 70,
        "early_entry_exception": False,
        "volume_boost": False
    },
    
    "RANGING": {
        "ma5_max_dev": 0.5,
        "ma5_min_dev": -1.0,
        "low_max_tier1": 1.8,
        "low_max_tier2": 2.0,
        "low_max_tier3": 2.5,
        "box_hardcut": True,
        "box_threshold": 75,
        "early_entry_exception": False,
        "volume_boost": False
    },
    
    "BREAKDOWN": {
        "ma5_max_dev": 0.3,
        "ma5_min_dev": -2.0,
        "low_max_tier1": 1.5,
        "low_max_tier2": 1.8,
        "low_max_tier3": 2.5,
        "box_hardcut": True,
        "box_threshold": 60,
        "early_entry_exception": False,
        "volume_boost": False
    },
    "DEFAULT": {
        "ma5_max_dev": 0.8,
        "ma5_min_dev": -1.0,
        "low_max_tier1": 2.5,
        "low_max_tier2": 3.0,
        "low_max_tier3": 4.0,
        "box_hardcut": True,
        "box_threshold": 75,
        "early_entry_exception": False,
        "volume_boost": False
    }
}

# Knee Mode 세부 조건 (페널티 예외 처리용)
KNEE_MIN_CONF_THRESHOLD = 0.33      # 무릎 모드일 때 완화된 신뢰도 기준
KNEE_LOW_GAP_MIN = 0.20             # 무릎 인식 저점대비 하한 (%) - 0.30->0.20
KNEE_LOW_GAP_MAX = 2.00             # 무릎 인식 저점대비 상한 (%) - HARD-GATE와 통일
KNEE_MA_DEV_ABS_MAX = 0.50          # MA5 이격 절대값 상한 (%)
KNEE_MA_GAP_AVG_MAX = 0.30          # MA5~20 평균 간격 상한 (%)
KNEE_ADX_MIN = 10.0                 # Knee 모드 ADX 하한
KNEE_ADX_MAX = 22.0                 # Knee 모드 ADX 상한
KNEE_VOL_RATIO_MIN = 0.40           # 거래량 하한
KNEE_VOL_RATIO_MAX = 2.50           # 거래량 상한

# ================== 15. 재시도 및 백오프 ==================
DEFAULT_RETRY_COUNT = 3  # 기본 재시도 횟수
DEFAULT_BACKOFF_FACTOR = 2  # 백오프 배수 (2배씩 증가)
DEFAULT_BACKOFF_BASE = 0.1  # 백오프 기본 시간 (0.1초)
DEFAULT_MAX_RETRY_WAIT = 10  # 최대 재시도 대기시간 (10초)
MAX_RECONNECT_ATTEMPTS = 10  # 최대 재연결 시도 횟수

# ================== 16. 시장 분석 ==================
CANDLES_FETCH_4H = 50  # 4시간봉 가져올 캔들 수
CANDLES_FETCH_1D = 100  # 일봉 가져올 캔들 수
DEFAULT_VOLATILITY_WINDOW = 20  # 변동성 계산 윈도우
ATR_MIN_PERIODS = 1  # ATR 최소 기간
MIN_CANDLES_FOR_REGIME = 50  # 체제 감지 최소 캔들
MIN_CANDLES_FOR_TRANSITION = 20  # 체제 전환 감지 최소 캔들
MIN_CANDLES_FOR_PATTERN = 2  # 패턴 인식 최소 캔들
MIN_CANDLES_REQUIRED = 30  # 분석 최소 필요 캔들

# ================== 17. 타임프레임별 MA 기간 ==================
MA_PERIODS_BY_TIMEFRAME = {
    '1m': {'short': 3, 'medium': 6, 'long': 10, 'trend': 30},
    '3m': {'short': 5, 'medium': 10, 'long': 20, 'trend': 40},
    '5m': {'short': 6, 'medium': 12, 'long': 24, 'trend': 72},
    '10m': {'short': 6, 'medium': 12, 'long': 24, 'trend': 72},
    '15m': {'short': 4, 'medium': 8, 'long': 16, 'trend': 48},
    '30m': {'short': 4, 'medium': 8, 'long': 16, 'trend': 48},
    '1h': {'short': 6, 'medium': 12, 'long': 24, 'trend': 72},
    '4h': {'short': 6, 'medium': 12, 'long': 24, 'trend': 72},
    '1d': {'short': 5, 'medium': 20, 'long': 60, 'trend': 120}
}

# ================== 18. 타임프레임별 통합 설정 ==================
TIMEFRAME_CONFIGS = {
    '1m': {
        'min_candles': 60,
        'update_interval': 30,
        # 'timeout_hours': 2,  # ❌ SSoT 위반 - config_{tf}.ini [EXIT]로 이동
        'whipsaw_sensitivity': 0.3,
        'atr_multipliers': {'stop_loss': 1.5, 'take_profit': 2.0, 'trailing_stop': 1.0},
        'confidence_threshold': 0.55,
        'min_volume': 1000000000,
        'market_update_interval': 10,
        'rolling_window': 30,
        'trading_interval': 30,
        'ma_periods': {'short': 5, 'long': 20}
    },
    '3m': {
        'min_candles': 60,
        'update_interval': 45,
        # 'timeout_hours': 1.0,  # ❌ SSoT 위반 - config_{tf}.ini [EXIT]로 이동
        'whipsaw_sensitivity': 0.45,
        'atr_multipliers': {'stop_loss': 1.8, 'take_profit': 3.5, 'trailing_stop': 1.2},
        'min_volume': 2000000000,
        'market_update_interval': 15,
        'rolling_window': 45,
        'trading_interval': 30,
        'ma_periods': {'short': 5, 'long': 20}
    },
    '5m': {
        'min_candles': 100,
        'update_interval': 60,
        # 'timeout_hours': 2.0,  # ❌ SSoT 위반 - config_{tf}.ini [EXIT]로 이동
        'whipsaw_sensitivity': 0.5,
        'atr_multipliers': {'stop_loss': 2.0, 'take_profit': 4.0, 'trailing_stop': 1.5},
        'min_volume': 3000000000,
        'market_update_interval': 20,
        'rolling_window': 60,
        'trading_interval': 60,
        'ma_periods': {'short': 20, 'long': 60}
    },
    '10m': {
        'min_candles': 100,
        'update_interval': 120,
        # 'timeout_hours': 3.0,  # ❌ SSoT 위반 - config_{tf}.ini [EXIT]로 이동
        'whipsaw_sensitivity': 0.55,
        'atr_multipliers': {'stop_loss': 2.2, 'take_profit': 4.5, 'trailing_stop': 1.7},
        'min_volume': 4000000000,
        'market_update_interval': 30,
        'rolling_window': 120,
        'trading_interval': 120,
        'ma_periods': {'short': 20, 'long': 50}
    },
    '15m': {
        'min_candles': 100,
        'update_interval': 180,
        # 'timeout_hours': 4.0,  # ❌ SSoT 위반 - config_{tf}.ini [EXIT]로 이동
        'whipsaw_sensitivity': 0.6,
        'atr_multipliers': {'stop_loss': 2.5, 'take_profit': 5.0, 'trailing_stop': 2.0},
        'min_volume': 4500000000,
        'market_update_interval': 30,
        'rolling_window': 180,
        'trading_interval': 180,
        'ma_periods': {'short': 20, 'long': 50}
    },
    '30m': {
        'min_candles': 120,
        'update_interval': 300,
        # 'timeout_hours': 6.0,  # ❌ SSoT 위반 - config_{tf}.ini [EXIT]로 이동
        'whipsaw_sensitivity': 0.65,
        'atr_multipliers': {'stop_loss': 2.5, 'take_profit': 5.0, 'trailing_stop': 2.0},
        'min_volume': 5000000000,
        'market_update_interval': 60,
        'rolling_window': 360,
        'trading_interval': 300,
        'ma_periods': {'short': 20, 'long': 50}
    },
    '1h': {
        'min_candles': 150,
        'update_interval': 600,
        # 'timeout_hours': 12.0,  # ❌ SSoT 위반 - config_{tf}.ini [EXIT]로 이동
        'whipsaw_sensitivity': 0.7,
        'atr_multipliers': {'stop_loss': 3.0, 'take_profit': 6.0, 'trailing_stop': 2.5},
        'min_volume': 5000000000,
        'market_update_interval': 120,
        'rolling_window': 720,
        'trading_interval': 600,
        'ma_periods': {'short': 20, 'long': 60}
    },
    '4h': {
        'min_candles': 150,
        'update_interval': 1800,
        # 'timeout_hours': 24.0,  # ❌ SSoT 위반 - config_{tf}.ini [EXIT]로 이동
        'whipsaw_sensitivity': 0.75,
        'atr_multipliers': {'stop_loss': 3.5, 'take_profit': 7.0, 'trailing_stop': 3.0},
        'min_volume': 5000000000,
        'market_update_interval': 240,
        'rolling_window': 1440,
        'trading_interval': 1800,
        'ma_periods': {'short': 20, 'long': 60}
    },
    '1d': {
        'min_candles': 200,
        'update_interval': 3600,
        # 'timeout_hours': 72.0,  # ❌ SSoT 위반 - config_{tf}.ini [EXIT]로 이동
        'whipsaw_sensitivity': 0.8,
        'atr_multipliers': {'stop_loss': 4.0, 'take_profit': 8.0, 'trailing_stop': 3.5},
        'min_volume': 5000000000,
        'market_update_interval': 1440,
        'rolling_window': 2880,
        'trading_interval': 3600,
        'ma_periods': {'short': 20, 'long': 60}
    }
}

# 60m과 240m 별칭 추가
TIMEFRAME_CONFIGS['60m'] = TIMEFRAME_CONFIGS['1h']
TIMEFRAME_CONFIGS['240m'] = TIMEFRAME_CONFIGS['4h']

# ================== 19. 신호 분석 ==================
SIGNAL_STRENGTH_MIN = -1.0  # 신호 강도 최소값 (매도 신호)
SIGNAL_STRENGTH_MAX = 1.0  # 신호 강도 최대값 (매수 신호)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🆕 타임프레임별 거래량 MA 기간 (단기는 짧게, 장기는 길게)
# - 목적: 3m 봉에서 급등 잔영 제거 + 빠른 반응
# - 3m: 20봉(60분) → 12봉(36분) 단축 ⬅️ 핵심 개선
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
VOLUME_MA_PERIODS = {
    '1m': 10,   # 10봉 (10분)
    '3m': 12,   # 12봉 (36분) ⬅️ 핵심: 급등 잔영 제거
    '5m': 15,   # 15봉 (75분)
    '10m': 18,  # 18봉 (180분)
    '15m': 20,  # 20봉 (300분)
    '30m': 20,  # 20봉 (600분)
    '1h': 20,   # 20봉 (20시간)
    '4h': 20,   # 20봉 (80시간)
    '1d': 20    # 20봉 (20일)
}
VOLUME_MA_PERIOD = 20  # 기본값 (fallback용)
WHIPSAW_LOOKBACK_CANDLES = 10  # 휩쏘 감지 룩백 캔들
WHIPSAW_MIN_DIRECTION_CHANGES = 5  # 휩쏘 판단 최소 방향전환
WHIPSAW_VOLATILITY_THRESHOLD_PCT = 2.5  # 휩쏘 변동성 임계값 (%)
VOLUME_SURGE_MIN = 1.5  # 최소 거래량 급증 배수
VOLUME_SURGE_THRESHOLD = 0.7  # 거래량 급증 임계값
VOLUME_PRICE_CHANGE_SCALE = 10.0  # 거래량-가격변화 스케일
BREAKTHROUGH_SCALE = 100.0  # 돌파 신호 스케일
COMPONENT_MIN_VALUE = 0.1  # 컴포넌트 최소값

# ================== 20. 봇 운영 ==================
DEFAULT_WAIT_TIME = 60  # 기본 대기시간 (초)
DEFAULT_UPDATE_INTERVAL = 10  # 기본 업데이트 간격 (초)
DEFAULT_MAX_ERRORS = 5  # 최대 오류 허용 횟수
DEFAULT_LOG_INTERVAL = 0.5  # 로그 출력 간격 (시간)
ANALYSIS_INTERVAL_4H = 4  # 4시간 분석 간격
ANALYSIS_INTERVAL_1H = 1  # 1시간 분석 간격
ANALYSIS_INTERVAL_24H = 24  # 24시간 분석 간격
WAIT_TIME_RISK_LIMIT = 300  # 리스크 한도 도달시 대기시간 (5분)
ERROR_RECOVERY_WAIT = 10  # 오류 복구 대기시간 (초)
BALANCE_UPDATE_INTERVAL = 10  # 잔고 업데이트 간격 (초)

# ================== 21. 성과 메트릭 ==================
SHARPE_RATIO_MIN = -1.0  # Sharpe Ratio 최소값
SHARPE_RATIO_MAX = 3.0  # Sharpe Ratio 최대값
CALMAR_RATIO_MIN = -0.5  # Calmar Ratio 최소값
CALMAR_RATIO_MAX = 2.0  # Calmar Ratio 최대값
EMA_ALPHA = 0.2  # 지수이동평균 알파값 (20%)
SHARPE_CRITICAL = -0.5  # 위험 Sharpe 기준
SHARPE_PENALTY_MULT = 0.7  # Sharpe 페널티 배수
BREAKDOWN_REGIME_MULT = 0.85  # 붕괴 체제 배수

# ================== 22. 성과 신뢰도 계산 ==================
SHARPE_SCORE_OFFSET = 1  # Sharpe 점수 오프셋
SHARPE_SCORE_DIVISOR = 4  # Sharpe 점수 제수
SHARPE_SCORE_MAX_CONTRIB = 0.25  # Sharpe 최대 기여도
EDGE_SCORE_MULTIPLIER = 5  # Edge 점수 배수
EDGE_SCORE_MAX_CONTRIB = 0.25  # Edge 최대 기여도
CONFIDENCE_SCORE_MIN = 0.2  # 최소 신뢰도 점수
CONFIDENCE_SCORE_MAX = 1.0  # 최대 신뢰도 점수

# ================== 25. MA 분석 ==================
MA_SIGNAL_WEAK_THRESHOLD = 0.3  # 약한 MA 신호 임계값
MA_SLOPE_TREND_THRESHOLD = 0.0015  # MA 기울기 추세 임계값 (0.2%->0.15%, B안: 균형형 완화)
MA_SIGNAL_ACTIONABLE_THRESHOLD = 0.3  # 실행 가능 신호 임계값
MA_SIGNAL_BULLISH_BASE = 0.7  # 상승 정배열 기본 신호
MA_SIGNAL_BEARISH_BASE = -0.7  # 하락 역배열 기본 신호
MA_SIGNAL_CROSS_GOLDEN = 0.6  # 골든크로스 신호
MA_SIGNAL_CROSS_DEAD = -0.6  # 데드크로스 신호
MA_LOOKBACK_SLOPE = 2  # MA 기울기 계산 룩백 캔들수
MA_CROSS_LOOKBACK = 3  # 크로스 체크 룩백

# ================== 26. 시스템 설정 ==================
SQLITE_TIMEOUT = 30  # SQLite 타임아웃 (초)
MAX_CANDLES_TO_STORE = 200  # 저장할 최대 캔들 수
SEPARATOR_LENGTH = 70  # 구분선 길이

# ================== 27. 15분봉 MA ==================
MA_15MIN_CANDLE_COUNT = 20  # 15분봉 캔들 수
MA_15MIN_SHORT = 10  # 15분봉 단기 MA
MA_15MIN_LONG = 20  # 15분봉 장기 MA
MA_15MIN_CACHE_MINUTES = 1  # 캐시 시간 (분)

# ================== 29. 웹소켓 설정 ==================
WEBSOCKET_BATCH_SIZE = 50  # 웹소켓 배치 크기
WEBSOCKET_PING_INTERVAL = 30  # 핑 간격 (초)
WEBSOCKET_PING_TIMEOUT = 10  # 핑 타임아웃 (초)

# ================== 30. 데이터베이스 ==================
DB_BATCH_FLUSH_SIZE = 30  # DB 배치 플러시 크기
DB_BATCH_FLUSH_INTERVAL = 30  # DB 배치 플러시 간격 (초)

# ================== 31. 병렬 처리 ==================
MAX_CONCURRENT_REQUESTS = 10  # 최대 동시 요청 수
SEMAPHORE_LIMIT = 10  # 세마포어 제한

# ================== 32. 기본 지표 값 ==================
DEFAULT_INDICATOR_VALUES = {
    'atr': 0.0,
    'noise_filter': DEFAULT_SCORE,
    'volatility': DEFAULT_VOLATILITY_VALUE,
    'momentum': 0.0,
    'adx': 20.0
}

# ================== 33. 메모리 정리 임계값 ==================
MEMORY_CLEANUP_THRESHOLD = 0.8  # 메모리 사용률 임계값 (80%)
MEMORY_CLEANUP_INTERVAL = 300  # 메모리 정리 간격 (5분)

# ================== 34. 포지션 사이징 ==================
VOLATILITY_ADJUSTMENT_FACTOR = 0.5  # 변동성 조정 계수
VOLATILITY_MULT_MIN = 0.7  # 최소 변동성 배수
VOLATILITY_MULT_MAX = 1.3  # 최대 변동성 배수
KELLY_MULTIPLIER_FACTOR = 0.3  # Kelly 배수 계수
CONFIDENCE_MULT_BASE = 0.8  # 신뢰도 기본 배수
CONFIDENCE_MULT_SCALE = 0.4  # 신뢰도 스케일
CONFIDENCE_POWER = 2.0  # 신뢰도 제곱값

# ================== 35. 신호 컴포넌트 ==================
DYNAMIC_THRESHOLD_BASE_WEIGHT = 0.7  # 동적 임계값 기본 가중치
DYNAMIC_THRESHOLD_CALCULATED_WEIGHT = 0.3  # 동적 임계값 계산 가중치
NOISE_FILTER_MIN_THRESHOLD = 0.2  # 노이즈 필터 최소 임계값

# ================== 36. 지지/저항 ==================
SUPPORT_RESISTANCE_LOOKBACK = 120  # 지지/저항 룩백 기간, 3m × 120 = 6시간
SUPPORT_RESISTANCE_HIGH_PERCENTILE = 90  # 저항선 백분위수
SUPPORT_RESISTANCE_LOW_PERCENTILE = 10  # 지지선 백분위수
BREAKTHROUGH_MULTIPLIER = 1.003  # 돌파 배수 (0.3% 초과)

# ================== 37. 캔들 패턴 ==================
HAMMER_BODY_MULTIPLIER = 2.0  # 해머 몸통 배수
HAMMER_UPPER_SHADOW_MULTIPLIER = 0.5  # 해머 윗꼬리 배수
HAMMER_BULLISH_SIGNAL = 0.6  # 해머 상승 신호
HAMMER_BEARISH_SIGNAL = -0.4  # 슈팅스타 하락 신호
ENGULFING_BULLISH_SIGNAL = 0.8  # 상승 장악형 신호
ENGULFING_BEARISH_SIGNAL = -0.8  # 하락 장악형 신호
ENGULFING_BODY_RATIO = 1.5  # 장악형 몸통 비율

# ================== 38. 매집 패턴 ==================
ACCUMULATION_LOOKBACK = 8  # 매집 패턴 룩백
ACCUMULATION_VOLUME_SURGE = 2.2  # 매집 거래량 급증 (2.2배)
ACCUMULATION_NARROW_RANGE = 0.6  # 좁은 레인지 비율 (60%)
ACCUMULATION_STRONG_CLOSE = 0.75  # 강한 종가 위치 (75%)
ACCUMULATION_FULL_SCORE = 1.0  # 완전 매집 점수
ACCUMULATION_PARTIAL_SCORE = 0.5  # 부분 매집 점수

# ================== 39. 동적 임계값 ==================
DYNAMIC_THRESHOLD_HIGH_WIN = 0.65  # 높은 승률 기준 (65%)
DYNAMIC_THRESHOLD_LOW_WIN = 0.45  # 낮은 승률 기준 (45%)
DYNAMIC_THRESHOLD_MIN = 0.60  # 최소 동적 임계값
DYNAMIC_THRESHOLD_MAX = 0.85  # 최대 동적 임계값
DYNAMIC_THRESHOLD_RANGE = 0.25  # 동적 임계값 범위
DYNAMIC_THRESHOLD_WIN_DIVISOR = 0.2  # 승률 제수
DYNAMIC_THRESHOLD_MIN_TRADES = 10  # 최소 거래 수

# ================== 40. 체제별 조정 배수 ==================
REGIME_POSITION_MULTIPLIER_DEFAULT = 1.0  # 기본 포지션 배수
POSITION_SIZE_SELL_BUFFER = 0.95  # 매도 포지션 버퍼 (95%)
VOLATILITY_BASE_FACTOR = 1.0  # 변동성 기본 계수
VOLATILITY_DYNAMIC_THRESHOLD = 5  # 변동성 동적 임계값
VOLATILITY_DYNAMIC_MULTIPLIER = 2  # 변동성 동적 배수

# ================== 41. 시장 분석 샘플링 ==================
MARKET_ANALYSIS_ALT_SAMPLE = 5  # 알트코인 분석 샘플 수
# MARKET_CONDITION_BULL_THRESHOLD = 3  # 상승장 판단 임계값 (3개)
# MARKET_CONDITION_BEAR_THRESHOLD = 3  # 하락장 판단 임계값 (3개)

# ================== 42. 캐시 관리 ==================
CACHE_SLOT_INTERVAL = 5  # 캐시 슬롯 간격 (5분)

# ================== 45. 슬리피지 관련 상수 ==================
SLIPPAGE_MIN_PCT = 0.01  # 최소 슬리피지 (%)
SLIPPAGE_MAX_PCT = 0.5  # 최대 슬리피지 (%)
SLIPPAGE_VOLUME_TIER_BOUNDARIES = [10, 30, 50]  # 거래량 순위 경계
SLIPPAGE_VOLATILITY_THRESHOLD = 2.5  # 높은 변동성 임계값 (%)
SLIPPAGE_LARGE_ORDER_THRESHOLD = 10000000  # 대량 주문 임계값 (원)

# ================== 46. 부분 체결 관련 상수 ==================
PARTIAL_FILL_MIN_RATIO = 50  # 최소 체결률 (%)
PARTIAL_FILL_MAX_RATIO = 100  # 최대 체결률 (%)
PARTIAL_FILL_RETRY_MAX = 5  # 최대 재시도 횟수
PARTIAL_FILL_RETRY_INTERVAL = 1.0  # 재시도 간격 (초)

# ================== 47. 체결 지연 관련 상수 ==================
LATENCY_MIN_MS = 10  # 최소 지연 (밀리초)
LATENCY_MAX_MS = 5000  # 최대 지연 (밀리초)
LATENCY_CONGESTION_THRESHOLD = 100  # 네트워크 혼잡 임계값
LATENCY_CRITICAL_WARNING_MS = 500  # 경고 임계값 (밀리초)

# ================== 48. 업비트 호가 단위 ==================
UPBIT_TICK_SIZES = [
    (2000000, 1000),
    (1000000, 500),
    (500000, 100),
    (100000, 50),
    (10000, 10),
    (1000, 5),
    (100, 1),
    (10, 0.1),
    (1, 0.01),
    (0, 0.001)
]

# ================== 49. 리얼리티 프리셋 레벨 ==================
REALITY_PRESET_NONE = 'none'
REALITY_PRESET_LOW = 'low'
REALITY_PRESET_MEDIUM = 'medium'
REALITY_PRESET_HIGH = 'high'
REALITY_PRESET_EXTREME = 'extreme'

# ================== 50. 시뮬레이션 통계 키 ==================
SIM_STAT_TOTAL_SLIPPAGE = 'total_slippage'
SIM_STAT_PARTIAL_FILLS = 'total_partial_fills'
SIM_STAT_AVG_FILL_RATIO = 'avg_fill_ratio'
SIM_STAT_AVG_LATENCY = 'avg_latency_ms'
SIM_STAT_ORDER_COUNT = 'order_count'

# ================== 51. 시뮬레이션 시드 관련 상수 ==================
SEED_MODE_FIXED = 'fixed'
SEED_MODE_DAILY = 'daily'
SEED_MODE_RANDOM = 'random'
DEFAULT_SEED_VALUE = 42
SEED_DATE_FORMAT = '%Y%m%d'

# ================== 52. 시나리오 프로파일 ==================
SCENARIO_NORMAL = 'normal'
SCENARIO_BULL_RUN = 'bull_run'
SCENARIO_BEAR_MARKET = 'bear_market'
SCENARIO_FLASH_CRASH = 'flash_crash'
SCENARIO_HIGH_VOLATILITY = 'high_volatility'

# ================== 53. 시나리오별 기본 배수 ==================
SCENARIO_MULTIPLIERS = {
    'normal': {'slippage': 1.0, 'fill_ratio': 1.0, 'latency': 1.0},
    'bull_run': {'slippage': 1.5, 'fill_ratio': 0.9, 'latency': 1.2},
    'bear_market': {'slippage': 1.8, 'fill_ratio': 0.85, 'latency': 1.3},
    'flash_crash': {'slippage': 3.0, 'fill_ratio': 0.6, 'latency': 2.0},
    'high_volatility': {'slippage': 2.0, 'fill_ratio': 0.8, 'latency': 1.5}
}

# ================== 54. 안전장치 상수 (성과 모듈) ==================
SAFE_MIN_SAMPLES = 100  # 조정 최소 샘플
SAFE_EMA_ALPHA = 0.1  # EMA 스무딩
SAFE_MAX_DAILY_ADJ = 0.05  # 일일 최대 조정 (±5%)
SAFE_MIN_WIN_RATE = 0.3  # 최소 승률
SAFE_MIN_EXPECTED = 0.0  # 최소 기대값
SAFE_VALIDATION_PERIOD = 50  # 검증 기간
SAFE_MAX_WEIGHT_CHANGE = 0.2  # 최대 변화 (±20%)
SAFE_CONFIDENCE_THRESH = 0.6  # 조정 신뢰도 임계값
SAFE_ROLLBACK_THRESHOLD = -0.02  # 롤백 임계값 (-2%)

# ================== 55. 포트폴리오 리스크 관리 상수 ==================
PORTFOLIO_MAX_EXPOSURE = 0.60  # 최대 포트폴리오 노출도 (30%->60%)
PORTFOLIO_MAX_SECTOR = 0.60    # 단일 섹터 최대 비중 (15%->60%)
PORTFOLIO_MAX_CORRELATED = 3  # 상관관계 높은 포지션 최대 개수
VAR_CONFIDENCE_LEVEL = 0.95  # VaR 신뢰수준 (95%)
VAR_LOOKBACK_DAYS = 30  # VaR 계산 기간 (30일)
CORRELATION_HIGH_THRESHOLD = 0.7  # 높은 상관계수 기준 (0.7)
CORRELATION_WINDOW = 20  # 상관계수 계산 윈도우 (20일)
SECTOR_RISK_MULTIPLIER = 0.8  # 동일 섹터 리스크 배수
PORTFOLIO_RISK_SCORE_MAX = 100  # 포트폴리오 리스크 점수 최대값

# 마켓 관련
DEFAULT_MIN_VOLUME_KRW = 5000000000  # 기본 최소 거래대금 (50억원)
MARKET_SELECTION_CANDLES = 40  # 마켓 선정시 사용할 캔들 수
ADX_DEFAULT_VALUE = 20.0  # ADX 기본값

# ================== 56. 타임프레임별 MA 분석 임계값 ==================
MA_ANALYSIS_THRESHOLDS = {
    '1m': {
        'divergence_oversold': -1.5,
        'divergence_bounce': -0.5,
        'divergence_overbought': 1.5,
        'divergence_heating': 0.5,
        'signal_weak_threshold': 0.05,
        'actionable_min_signal': 0.03,
        'actionable_min_divergence': 0.5,
        'trend_threshold': 0.001,
        'cross_lookback': 2,
    },
    '3m': {
        'divergence_oversold': -2.0,
        'divergence_bounce': -0.8,
        'divergence_overbought': 2.0,
        'divergence_heating': 0.8,
        'signal_weak_threshold': 0.08,
        'actionable_min_signal': 0.05,
        'actionable_min_divergence': 0.8,
        'trend_threshold': 0.0015,
        'cross_lookback': 2,
    },
    '5m': {
        'divergence_oversold': -2.5,
        'divergence_bounce': -1.0,
        'divergence_overbought': 2.5,
        'divergence_heating': 1.0,
        'signal_weak_threshold': 0.10,
        'actionable_min_signal': 0.08,
        'actionable_min_divergence': 1.0,
        'trend_threshold': 0.002,
        'cross_lookback': 3,
    },
    '15m': {
        'divergence_oversold': -3.0,
        'divergence_bounce': -1.5,
        'divergence_overbought': 3.0,
        'divergence_heating': 1.5,
        'signal_weak_threshold': 0.15,
        'actionable_min_signal': 0.10,
        'actionable_min_divergence': 1.5,
        'trend_threshold': 0.003,
        'cross_lookback': 3,
    },
    '30m': {
        'divergence_oversold': -4.0,
        'divergence_bounce': -2.0,
        'divergence_overbought': 4.0,
        'divergence_heating': 2.0,
        'signal_weak_threshold': 0.20,
        'actionable_min_signal': 0.15,
        'actionable_min_divergence': 2.0,
        'trend_threshold': 0.004,
        'cross_lookback': 4,
    },
    '1h': {
        'divergence_oversold': -5.0,
        'divergence_bounce': -2.5,
        'divergence_overbought': 5.0,
        'divergence_heating': 2.5,
        'signal_weak_threshold': 0.25,
        'actionable_min_signal': 0.20,
        'actionable_min_divergence': 2.5,
        'trend_threshold': 0.005,
        'cross_lookback': 4,
    },
    '4h': {
        'divergence_oversold': -6.0,
        'divergence_bounce': -3.0,
        'divergence_overbought': 6.0,
        'divergence_heating': 3.0,
        'signal_weak_threshold': 0.30,
        'actionable_min_signal': 0.25,
        'actionable_min_divergence': 3.0,
        'trend_threshold': 0.008,
        'cross_lookback': 5,
    },
    '1d': {
        'divergence_oversold': -7.0,
        'divergence_bounce': -3.5,
        'divergence_overbought': 7.0,
        'divergence_heating': 3.5,
        'signal_weak_threshold': 0.35,
        'actionable_min_signal': 0.30,
        'actionable_min_divergence': 3.5,
        'trend_threshold': 0.010,
        'cross_lookback': 5,
    }
}

# 폴백 값 (기본값)
MA_ANALYSIS_DEFAULT = {
    'divergence_oversold': -5.0,
    'divergence_bounce': -1.5,
    'divergence_overbought': 5.0,
    'divergence_heating': 3.0,
    'signal_weak_threshold': 0.30,
    'actionable_min_signal': 0.10,
    'actionable_min_divergence': 2.0,
    'trend_threshold': 0.005,
    'cross_lookback': 3,
}

# ================== 57. ATR 관련 상수 ==================
ATR_DEFAULT_PERIOD = 14  # ATR 계산 기간
ATR_MIN_THRESHOLD_PCT = 0.5  # 최소 변동성 임계값 (%)
ATR_MAX_THRESHOLD_PCT = 5.0  # 최대 변동성 임계값 (%)
ATR_STOP_LOSS_MULTIPLIER = 2.0  # ATR 손절 배수
ATR_TAKE_PROFIT_MULTIPLIER = 3.0  # ATR 익절 배수
ATR_TRAILING_STOP_MULTIPLIER = 1.5  # ATR 추적손절 배수
ATR_HIGH_POSITION_REDUCTION = 1.0  # 고변동성 포지션 축소
ATR_LOW_POSITION_EXPANSION = 0.8  # 저변동성 포지션 확대

# 타임프레임별 ATR 배수 설정
ATR_MULTIPLIERS_BY_TIMEFRAME = {
    '1m': {'stop_loss': 1.5, 'take_profit': 2.0, 'trailing_stop': 1.0},
    '3m': {'stop_loss': 1.8, 'take_profit': 2.5, 'trailing_stop': 1.2},
    '5m': {'stop_loss': 2.0, 'take_profit': 3.0, 'trailing_stop': 1.5},
    '10m': {'stop_loss': 2.2, 'take_profit': 3.5, 'trailing_stop': 1.7},
    '15m': {'stop_loss': 2.5, 'take_profit': 4.0, 'trailing_stop': 2.0},
    '30m': {'stop_loss': 2.5, 'take_profit': 4.0, 'trailing_stop': 2.0},
    '1h': {'stop_loss': 3.0, 'take_profit': 5.0, 'trailing_stop': 2.5},
    '4h': {'stop_loss': 3.5, 'take_profit': 6.0, 'trailing_stop': 3.0},
    '1d': {'stop_loss': 4.0, 'take_profit': 7.0, 'trailing_stop': 3.5}
}

# ATR 변동성 레벨 분류
# 📊 2025-10-25 시장 데이터 기반 재조정 (Phase 1: 보수적 완화)
# 근거: 실제 시장 ATR 평균 0.18%, 대부분 0.10~0.30% 구간 집중
# 로그 분석: 기존 0.25% 임계값으로 80% 기회 차단 → 0.15%로 완화
ATR_LEVEL_VERY_LOW = 0.10   # 매우 낮은 변동성 (%) - 0.25 → 0.15 → 0.10 (평균보다 약간 낮게)
ATR_LEVEL_LOW = 1.0         # 낮은 변동성 (%) - 1.0 → 0.25 → 1.0 (기존 VERY_LOW 수준)
ATR_LEVEL_NORMAL = 2.5      # 정상 변동성 (%) - 2.5 → 0.40 → 2.5 (상위 30% 커버)
ATR_LEVEL_HIGH = 4.0        # 높은 변동성 (%) - 4.0 → 0.60 → 4.0 (고변동성 구간)
ATR_LEVEL_EXTREME = 5.0     # 극단적 변동성 (%) - 복구: LOW와 중복 제거

# ATR 신호 강도 (변경 없음 - 기존 로직 유지)
ATR_SIGNAL_VERY_LOW = 0.0    # 변동성 너무 낮음 (진입 회피) # 0.0 필터 차단
ATR_SIGNAL_LOW = 0.5         # 횡보 가능성 (횡보 전략) # 0.3 → 0.5 상향
ATR_SIGNAL_NORMAL = 0.8      # 정상 (중립) # 0.5 → 0.8 상향 (이상적)
ATR_SIGNAL_HIGH = 0.9        # 추세 가능성 (추세 전략) # 0.8 → 0.9 상향
ATR_SIGNAL_EXTREME = 0.0     # 변동성 과다 (진입 회피) # 필터 차단

# 메모리 제한
MEMORY_BOUNDS = {
    'trade_history': 1000,
    'signal_history': 100,
    'error_history': 100,
    'max_signal_types': 50,
    'max_ma_cache': 100,
    'max_strategy_perf': 20,
    'cache_max_size': 200,
    
    # 선행 정의(496-510)에 있던 운영 키 복구
    'performance_buffer': 60,
    'volatility_history': 50,
    'condition_history': 30,
    'regime_history': 50,
    'transition_history': 30,
    'drawdown_history': 50,
    'lock_cleanup_threshold': 30,
    'ws_message_buffer': 500,
    'batch_buffer': 50
}

# ========================================
# MA 이격도 임계값 (체제별 차별화)
# ========================================
MA_DIVERGENCE_THRESHOLDS = {
    # 횡보장: 좁은 범위 (정배열이어도 변동 작음)
    'RANGING': {
        '1m': {'oversold': -2.5, 'overbought': 3.0},
        '3m': {'oversold': -3.0, 'overbought': 3.5},
        '5m': {'oversold': -3.5, 'overbought': 4.0},
        '10m': {'oversold': -4.0, 'overbought': 4.5},
        '15m': {'oversold': -4.5, 'overbought': 5.0},
        'default': {'oversold': -3.0, 'overbought': 4.0}
    },
    
    # 🔥 상승장: 축소 범위 (하락 추세 오판 방지)
    'TRENDING_UP': {
        '1m': {'oversold': -0.8, 'overbought': 1.2},   # 🔥 변경: -3~2 → -0.8~1.2
        '3m': {'oversold': -0.6, 'overbought': 1.0},   # 🔥 변경: -4~2.5 → -0.6~1.0 (핵심)
        '5m': {'oversold': -0.8, 'overbought': 1.2},   # 🔥 변경: -5~3 → -0.8~1.2
        '10m': {'oversold': -1.0, 'overbought': 1.5},  # 🔥 변경: -6~4 → -1.0~1.5
        '15m': {'oversold': -1.0, 'overbought': 1.5},  # 🔥 변경: -7~5 → -1.0~1.5
        '30m': {'oversold': -1.2, 'overbought': 1.8},  # 🔥 추가: 30m 설정
        '1h': {'oversold': -1.5, 'overbought': 2.0},   # 🔥 추가: 1h 설정
        'default': {'oversold': -0.8, 'overbought': 1.2}  # 🔥 변경: -2~6 → -0.8~1.2
    },
    
    # 하락장: 역이격도 (MA 아래로 벌어짐) - 변경 없음
    'TRENDING_DOWN': {
        '1m': {'oversold': -5.0, 'overbought': 1.0},
        '3m': {'oversold': -6.0, 'overbought': 1.5},
        '5m': {'oversold': -7.0, 'overbought': 2.0},
        '10m': {'oversold': -8.0, 'overbought': 2.5},  # 🔥 추가
        '15m': {'oversold': -9.0, 'overbought': 3.0},  # 🔥 추가
        'default': {'oversold': -6.0, 'overbought': 2.0}
    },
    
    # 돌파: 매우 넓은 범위 (급등 허용) - 변경 없음
    'BREAKOUT': {
        '1m': {'oversold': 0.0, 'overbought': 8.0},
        '3m': {'oversold': 0.0, 'overbought': 10.0},
        '5m': {'oversold': 0.0, 'overbought': 12.0},
        '10m': {'oversold': 0.0, 'overbought': 14.0},  # 🔥 추가
        '15m': {'oversold': 0.0, 'overbought': 15.0},  # 🔥 추가
        'default': {'oversold': 0.0, 'overbought': 10.0}
    },
    
    # 붕괴: 진입 금지 - 변경 없음
    'BREAKDOWN': {
        'default': {'oversold': -999, 'overbought': 999}
    }
}

# MA 이탈 관련 상수 (기존 값 오버라이드)
MA_EXIT_THRESHOLDS = {
    '1m': -2.0,
    '3m': -3.5, 
    '5m': -5.0,
    '10m': -6.0,
    '15m': -7.0
}

MA15_SAFETY_BUFFERS = {
    '1m': 0.003,
    '3m': 0.005,
    '5m': 0.007,
    '10m': 0.010,
    '15m': 0.015
}

# ========================================
# Entry / Penalty 튜닝 상수 (2025-11-16)
# ========================================

# 저점대비 판단용: 최근 N봉 저점 기준
ENTRY_LOW_LOOKBACK_BARS = 20  # 3m 기준 20봉 = 60분

# 체제별 저점대비 상한 (무릎 이후 너무 멀어진 구간 필터)
ENTRY_LOW_MAX_TRENDING_UP = 2.0   # 상승장: +2.0%까지 허용
ENTRY_LOW_MAX_BREAKOUT    = 2.5   # 돌파장: +2.5%까지 허용
ENTRY_LOW_MAX_RANGING     = 2.0   # 횡보장: +2.0%까지 (가자미근 기준과 통일)
ENTRY_LOW_MAX_DEFAULT     = 2.0   # 그 외: +2.0% (가자미근 기준과 통일)

# 강한 신호 정의 (정배열/골든크로스)
STRONG_ALIGNMENT_LEVEL    = 4     # alignment_level ≥ 4 이면 강한 정배열
STRONG_GOLDEN_COUNT       = 2     # 골든크로스 2개 이상이면 강한 신호

# 강한 신호에 대한 페널티 하한 (과도한 누적 페널티 방지용)
PENALTY_FLOOR_STRONG_SIGNAL = 0.85  # 최종 penalty_factor 최소 0.85 보장

# ==========================================
#  Penalty Weight System (감점 기반) - 2025-11-16
# ==========================================

PENALTY_WEIGHTS = {
    'ma_reverse': 0.25,         # MA 역배열 (치명)
    'ma60_below': 0.20,         # MA60 하회
    'long_downtrend': 0.15,     # 장기하락세
    'adx_weak': 0.03,           # 🔥 0.05 → 0.03 (초단타 ADX 완화)
    'adx_falling': 0.08,        # ADX 급락
    'ma5_over_dev': 0.10,       # MA5 과이격
    'ma5_under_dev': 0.12,      # MA5 과하회
    'price_falling': 0.15,      # 하락 진행
    'late_rebound': 0.08,       # 늦은 반등
    'no_pattern': 0.07,         # V/U 패턴 부재
    'overbought': 0.10,         # 과매수
    'ma20_falling': 0.08,       # MA20 하락
    'vol_decel': 0.05,          # 거래량 둔화
    'vol_decel_knee': 0.01      # 거래량 둔화 (무릎모드)
}

# ========================================
# 가격 포맷 유틸리티 함수
# ========================================
def format_price(price: float) -> str:
    """극저가 코인 가격 포맷"""
    if price == 0:
        return "0"
    elif price < 0.001:
        return f"{price:.6f}"
    elif price < 0.01:
        return f"{price:.5f}"
    elif price < 1:
        return f"{price:.4f}"
    elif price < 100:
        return f"{price:.2f}"
    elif price < 10000:
        return f"{price:.1f}"
    else:
        return f"{price:.0f}"

# ========================================
# 🆕 불장(Bull Mode) 관련 상수 - 2025-11-26
# ========================================

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1. 글로벌 불장 판단 가중치
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BULL_WEIGHT_BTC_TREND = 0.40         # BTC 4H 추세 가중치 (상향: 30%→40%)
BULL_WEIGHT_ALT_TREND = 0.40         # 알트 Top50 4H 추세 가중치 (상향: 30%→40%)
BULL_WEIGHT_DOMINANCE = 0.10         # BTC vs 알트 상대성과 가중치 (하향: 20%→10%)
BULL_WEIGHT_LIQUIDITY = 0.10         # 알트 유동성 증가율 가중치 (하향: 20%→10%)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2. 불장 히스테리시스 (빠른 진입 / 느린 해제)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BULL_ENTRY_THRESHOLD = 0.50          # 불장 진입 스코어 임계값
BULL_EXIT_THRESHOLD = 0.35           # 불장 해제 스코어 임계값
BULL_ENTRY_COUNT = 2                 # 불장 진입 필요 횟수 (빠른 감지)
BULL_EXIT_COUNT = 5                  # 불장 해제 필요 횟수 (느린 탈출)
BULL_FORCE_EXIT_RATIO = 0.15         # 강제 OFF: 상승 종목 15% 미만

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 3. BTC 도미넌스 변화율 임계값
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DOMINANCE_FALL_THRESHOLD = -0.5      # 도미넌스 -0.5% 이하 = 알트 불장
DOMINANCE_RISE_THRESHOLD = 0.5       # 도미넌스 +0.5% 이상 = 알트 위축

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 4. 심볼 로컬 불장 3단계 기준 (하이브리드 - 균형)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 철학:
#   - Option C: Level 1, 2에 ATR 필터 추가 (노이즈 차단)
#   - 업비트 현실: Level 3 기준 유지 (실제 불장 포착)
#   - 업비트 3m 타임프레임 4H 데이터 기준
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SYMBOL_BULL_LEVEL_1 = {
    # 초기 상승 (약한 불장) - 조기 포착 + ATR 필터로 노이즈 차단
    'adx_min': 18.0,           # 방향성 초기 발현
    'atr_min': 0.20,           # 🔥 신설 (노이즈 필터)
    'change_24h_min': 3.0,     # 초기 상승 구간
    'relax_factor': 0.9        # 완화 10%
}

SYMBOL_BULL_LEVEL_2 = {
    # 강한 상승 (중간 불장) - 강세 구간 + ATR 필터로 노이즈 차단
    'adx_min': 23.0,           # 명확한 추세 형성
    'atr_min': 0.30,           # 🔥 신설 (노이즈 필터)
    'change_24h_min': 6.0,     # 강세 구간
    'relax_factor': 0.8        # 완화 20%
}

SYMBOL_BULL_LEVEL_3 = {
    # 폭발적 상승 (강한 불장 / 스캘핑 최적)
    # 업비트 실제 불장 구간 (ATR 0.45~0.80, 24h 10~15%)
    'adx_min': 28.0,           # 강한 추세 확정
    'atr_min': 0.45,           # 🔥 업비트 실제 불장 하한
    'change_24h_min': 11.0,    # 🔥 업비트 실제 불장 구간
    'relax_factor': 0.65       # 완화 35%
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 5. 불장 강도 계산 상수
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BULL_STRENGTH_BASE = 1.0               # 기본 불장 강도 (글로벌 불장 ON시)
BULL_STRENGTH_SYMBOL_MULTIPLIER = 0.3  # 🔥 0.2 → 0.3 (Level 2도 승격)
BULL_STRENGTH_MAX = 1.6                # 최대 불장 강도

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 6. 불장 모드 진입 완화 계수
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BULL_RELAX_KNEE_MAX = 2.7            # 무릎 상한 최대값 (%)
BULL_RELAX_KNEE_FACTOR = 0.15        # 무릎 상한 완화 계수
BULL_RELAX_VOL_MIN = 0.25            # 거래량 최소값 (배수)
BULL_RELAX_VOL_FACTOR = 0.15         # 거래량 완화 계수
BULL_RELAX_PATTERN_BASE = 0.95       # 패턴 페널티 기본값
BULL_RELAX_PATTERN_MAX = 0.04        # 패턴 페널티 최대 완화
BULL_RELAX_THRESHOLD_FACTOR = 0.10   # Threshold 완화 계수

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 7. 불장 무릎 상한 (BULL_MARKET 전용) - Option B 균형형
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
KNEE_LOW_MAX_BULL = 4.0              # 불장 무릎 상한 4.0% (공격적 완화)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🎯 2-Track 모니터링 시스템 (관측 범위 확장)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MainPool: 거래대금 기반 (기존 유지)
MONITORING_TOP_VOLUME_90M = 30  # 90분봉 Top 30
MONITORING_TOP_VOLUME_4H = 20   # 4시간봉 Top 20

# WatchList: 변동률 기반 (관측 전용)
WATCHLIST_CHANGE_15M_MIN = 3.0   # 15분 최소 변동률 +3%
WATCHLIST_CHANGE_15M_MAX = 25.0  # 15분 최대 변동률 +25% (급등 관측 허용)
WATCHLIST_CHANGE_30M_MIN = 4.0   # 30분 최소 변동률 +4%
WATCHLIST_CHANGE_30M_MAX = 35.0  # 30분 최대 변동률 +35% (급등 관측 허용)

# 추가 필터
WATCHLIST_MIN_VOLUME_RATIO = 0.5    # 최소 거래량 비율 (MA 대비)
WATCHLIST_MIN_PRICE = 0             # 가격대 일괄 제외 없음
WATCHLIST_LIQUIDITY_PCTILE_CUT = 0.60   # 🛡️ 시장 하위60% 거래대금 배제(백분위=하락장 자동적응)
WATCHLIST_WATCH_DURATION_HOURS = 2.0  # 추적 지속 시간 (시간)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 거래 가능 최소 가격 (시스템 안정성 필터)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
MIN_TRADE_PRICE = 0.000001          # 0 및 비정상값 제외, 소수점 6자리까지 포함

# 🛡️ 보호 장치 ①: Track2 수 상한
MAX_MOMENTUM_WATCHLIST = 5  # 동시 추적 최대 5개

# Track2 우선순위 정렬 기준
MOMENTUM_PRIORITY_WEIGHTS = {
    'change_15m': 0.3,      # 15분 변동률 가중치
    'change_30m': 0.3,      # 30분 변동률 가중치
    'volume_ratio': 0.2,    # 거래량 비율 가중치
    'tier': 0.2             # TIER 등급 가중치
}

# 🛡️ 보호 장치 ③: Track2 → Track1 승격 조건
PROMOTION_MIN_VOLUME_RATIO = 0.8      # 승격 최소 거래량 비율
PROMOTION_MIN_VOLUME_ACCEL = 0.5      # 승격 최소 거래량 가속도

# 🔧 보완 ①: 승격 후 쿨다운 (가짜 반등 방지)
PROMOTION_COOLDOWN_CANDLES = 2        # 승격 후 최소 대기 캔들
PROMOTION_COOLDOWN_MINUTES = 6        # 승격 후 최소 대기 시간 (분)

# 스캔 주기
MOMENTUM_SCAN_INTERVAL_MINUTES = 5  # 5분마다 스캔

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 전략 타임프레임 분류
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# 단타 전략 타임프레임 (공격적 진입/빠른 청산)
SHORT_TERM_TIMEFRAMES = ["1m", "3m", "5m"]

# 중기 전략 타임프레임 (안정적 추세 추종)
MEDIUM_TERM_TIMEFRAMES = ["10m", "15m", "30m", "1h"]

# 장기 전략 타임프레임 (스윙 트레이딩)
LONG_TERM_TIMEFRAMES = ["4h", "1d"]

# A그룹 의사결정 상수
GAJAMI_MIN_GAP = 0.3
GAJAMI_MAX_GAP = 1.5
MIN_VOLUME_RATIO = 1.5
BEARISH_PENALTY = 0.5
LOW_VOLUME_PENALTY = 0.7

# B그룹 (보조/완화) 기본값
DEFAULT_VOLUME_RATIO_SMOOTH = 0.0
DEFAULT_TRIMMED_BOX_APPLIED = False
DEFAULT_GLOBAL_KNEE_VOLUME_RELAXED = False

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 최종 판결 진입 신뢰도 (FINAL-ENTRY Override)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 철학: "최종 판결" 신호는 임계값 체크를 우회하지만
#       포지션 크기/리스크 관리를 위해 별도 신뢰도 유지
# 
# 현재: 모두 0.50으로 통일 (일반 진입과 동일)
# 향후: 필요시 개별 조정 가능 (예: BOX=0.35, TD=0.40)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CONFIDENCE_BOX_ENTRY = 0.50      # 박스 하단 진입 (현재 일반과 동일)
CONFIDENCE_TD_BASELINE = 0.50    # 골든크로스 기본 추종 (현재 일반과 동일)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 차단 사유 메시지 표준화 (SSoT)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 목적: 단일 계층 사유만 사용 (복합 사유 금지)
# 원칙: 영문 코드형 상수 + 수치 정보 통일
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# 1️⃣ 체제 관련 (최우선)
BLOCK_REASON_REGIME_BREAKDOWN = "REGIME_BREAKDOWN"  # 붕괴장
BLOCK_REASON_TD_GATE = "TD_GATE"                    # 하락전환

# 2️⃣ 가자미근 관련
BLOCK_REASON_NOT_SOLEUS = "NOT_SOLEUS"              # 가자미근 미형성
BLOCK_REASON_NOT_GAJAMI = "NOT_GAJAMI"              # 가자미근 아님 (RANGING)

# 3️⃣ 신뢰도 관련
BLOCK_REASON_CONF_LOW = "CONF_LOW"                  # 최종 신뢰도 부족
BLOCK_REASON_EEE_LOW = "EEE_LOW"                    # EEE 신뢰도 부족

# 4️⃣ 리스크 관련
BLOCK_REASON_VOL_LOW = "VOL_LOW"                    # 거래량 부족
BLOCK_REASON_ATR_BLOCK = "ATR_BLOCK"                # ATR 차단
BLOCK_REASON_SELLING_PRESSURE = "SELLING_PRESSURE"  # 매도압력
BLOCK_REASON_STRONG_SELLING = "STRONG_SELLING"      # 강한 매도압력

# 5️⃣ MA 관련
BLOCK_REASON_TU_NO_MA = "TU_NO_MA"                  # MA 신호 부족
BLOCK_REASON_MA_REVERSE = "MA_REVERSE"              # MA 역배열
BLOCK_REASON_MA5_DIST_BREAK = "MA5_DIST_BREAK"      # MA5 이격 초과

# 6️⃣ 기타
BLOCK_REASON_OVER_RALLY = "OVER_RALLY_ADJ"          # 급등 후 조정
BLOCK_REASON_CRASH_GATE = "CRASH_GATE"              # 급락 차단
BLOCK_REASON_DEAD_BOUNCE = "DEAD_BOUNCE"            # 데드 바운스
BLOCK_REASON_RANGING_WEAK = "RANGING_WEAK"          # 횡보 약세

# ========================================
# END OF BULL MODE CONSTANTS
# ========================================