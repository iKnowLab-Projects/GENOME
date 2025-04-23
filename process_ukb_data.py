import pandas as pd
import numpy as np
import os
import sys
import itertools
from pathlib import Path

# MILTON 라이브러리 및 관련 모듈 임포트
sys.path.append("code/milton-release")
from milton import *
from milton.batch import *
from milton.processing import GenderSpecNAStrategy, CategoricalNAStrategy

import warnings
warnings.filterwarnings('ignore')

# 파일 경로 설정
GENOME_DATA_PATH = "/home/soyeon/workspace/data/GENOME/UKB_genome"  # .bed, .fam, .bim 파일이 있는 경로
DEMO_DATA_PATH = "/home/soyeon/workspace/data/GENOME/total_demo.txt"  # 역학 데이터 경로    # 확장자: .txt 혹은 .parquet
OUTPUT_BASE_PATH = "./results"  # 결과 저장 경로

# UKB_DATA_LOCATION 환경 변수 설정 (MILTON이 UKB 데이터를 찾을 수 있도록)
os.environ["UKB_DATA_LOCATION"] = GENOME_DATA_PATH

def prepare_demographic_data(demo_file):
    """
    역학 데이터(total_demo.txt)를 읽고 MILTON에서 사용할 수 있는 형식으로 변환합니다.
    
    Args:
        demo_file: 역학 데이터 파일 경로
    
    Returns:
        전처리된 데이터프레임
    """
    print(f"역학 데이터 파일 {demo_file} 로드 중...")
    
    # 파일이 존재하는지 확인
    if not os.path.exists(demo_file):
        raise FileNotFoundError(f"역학 데이터 파일을 찾을 수 없습니다: {demo_file}")
    
    # 첫 몇 줄을 읽어 구분자 추측
    with open(demo_file, 'r', encoding='utf-8') as f:
        first_lines = [next(f) for _ in range(5)]
    
    # 구분자 감지
    if '\t' in first_lines[0]:
        sep = '\t'
    elif ',' in first_lines[0]:
        sep = ','
    else:
        sep = None

    # print("파일의 총 데이터 수를 계산 중...(파일의 크기가 클 수록 시간이 오래 걸릴 수 있습니다.)")

    # file_size = os.path.getsize(demo_file)
    # with open(demo_file, 'r', encoding='utf-8') as f:
    #     total_rows = 0
    #     bytes_read = 0
    #     last_percent = 0
        
    #     header = next(f)
    #     bytes_read += len(header.encode('utf-8'))
        
    #     for line in f:
    #         total_rows += 1
    #         bytes_read += len(line.encode('utf-8'))
            
    #         current_percent = int(bytes_read * 100 / file_size)
    #         if current_percent >= last_percent + 10:
    #             print(f"진행 상황: {current_percent}% (현재까지 {total_rows}행 처리)")
    #             last_percent = current_percent

    # print(f"총 데이터 수: {total_rows}")
    
    # user_input = input(f"총 {total_rows}개의 행이 있습니다. 전체 데이터를 로드하시겠습니까? (1: 전체 데이터 로드, 2: 일부 데이터 로드): ")
    user_input = input("전체 데이터를 로드하시겠습니까? (1: 전체 데이터 로드, 2: 일부 데이터 로드): ")
    # user_input = '2'
    # rows_to_load = 1000
    total_rows = 502366
    if user_input == '1':
        print("전체 데이터를 로드합니다...")
        if sep:
            df = pd.read_csv(demo_file, sep=sep)
        else:
            df = pd.read_csv(demo_file, sep=' ')
    elif user_input == '2':
        while True:
            try:
                rows_to_load = input(f"몇 개의 데이터를 로드하시겠습니까?")
                rows_to_load = int(rows_to_load)
                if 1 <= rows_to_load <= total_rows:
                    break
                else:
                    print(f"1과 {total_rows} 사이의 값을 입력해주세요.")
            except ValueError:
                print("유효한 숫자를 입력해주세요.")
        
        print(f"처음 {rows_to_load}개의 행만 로드합니다...")
        if sep:
            df = pd.read_csv(demo_file, sep=sep, nrows=rows_to_load)
        else:
            df = pd.read_csv(demo_file, sep=' ', nrows=rows_to_load)
    else:
        print("유효한 입력이 아닙니다. 프로그램을 종료합니다.")
        sys.exit(1)
    
    print(f"역학 데이터 로드 완료. 행 수: {df.shape[0]}, 열 수: {df.shape[1]}")
    print(f"첫 5개 열: {df.columns[:5].tolist()}")

    # 열 형식 전처리
    print("데이터프레임 열 이름 형식 전처리 중...")
    renamed_columns = {}
    for col in df.columns:
        new_col = col

        # if col.startswith('X'):
        #     # new_col = ''.join(filter(str.isdigit, col))
        #     new_col = col[1:]

        # # X31.0.0 형식의 열을 X31-0.0 형식으로 변경
        # if '.' in new_col and new_col.count('.') >= 1:
        #     # 첫 번째 점(.)을 하이픈(-)으로 변경
        #     parts = new_col.split('.', 1)  # 첫 번째 점에서만 분리
        #     new_col = f"{parts[0]}-{parts[1]}"

        # 만약 X31-0.0 형식의 열이면 X31.0.0 형식으로 변경
        if '-' in new_col:
            parts = new_col.split('-', 1)
            new_col = f"{parts[0]}.{parts[1]}"

        renamed_columns[col] = new_col
        
    
    # 변경된 열 이름이 있으면 적용
    if renamed_columns:
        df = df.rename(columns=renamed_columns)
    
    # 필요한 열 확인 및 전처리
    # 실제 데이터 구조에 따라 이 부분을 조정해야 할 수 있음
    required_columns = ['eid'] # 최소한 ID 열이 필요함
    
    # 필요한 열이 있는지 확인
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"필수 열 '{col}'이 역학 데이터에 없습니다")
    
    # # 혼합된 타입이 있을 수 있는 'object' 타입의 열을 카테고리 타입으로 변환
    # for col in df.select_dtypes(include=['object']).columns:
    #     df[col] = df[col].astype(str).astype('category')
    
    # # 필요한 경우 특정 수치형 열도 카테고리로 변환 (예: 적은 수의 고유값을 가진 열)
    # for col in df.select_dtypes(include=['int64', 'float64']).columns:
    #     # 고유값이 적은 열만 카테고리로 변환 (예: 고유값이 100개 미만인 경우)
    #     if df[col].nunique() < 100:
    #         df[col] = df[col].astype(str).astype('category')

    for col in df.columns:
        df[col] = df[col].astype(str).astype('category')
    
    df = df.astype('category')
    
    # 역학 데이터를 UKB 포맷에 맞게 변환 (parquet 형식으로 저장)
    output_dir = os.path.join(os.path.dirname(demo_file), "ukb.parquet")
    os.makedirs(output_dir, exist_ok=True)
    # output_path = os.path.join(output_dir, "ukb_processed.parquet")
    # df.to_parquet(output_path, index=False)

    part_size = len(df) // 10
    output_paths = []
    
    for i in range(10):
        start_idx = i * part_size
        end_idx = (i + 1) * part_size if i < 9 else len(df)
        part_df = df.iloc[start_idx:end_idx]
        part_file = os.path.join(output_dir, f"part-{i}.parquet")
        part_df.to_parquet(part_file, index=False)
        output_paths.append(part_file)
        print(f"파트 {i} 저장 완료 ({(i+1)*10}%): {part_file} (행 수: {len(part_df)})")

    # output_path = os.path.join(output_dir, "part-0.parquet")
    # df.to_parquet(output_path, index=False)
    
    print(f"전처리된 역학 데이터를 {output_dir} 위치에 10개의 파트로 나누어 저장했습니다.")

    # Sample List 파일 생성
    sample_list = df['eid'].unique()
    sample_list = pd.DataFrame(sample_list, columns=['eid'])
    # txt 파일로 저장
    os.makedirs(os.path.join(os.path.dirname(demo_file), "sample_lists"), exist_ok=True)
    sample_list.to_csv(os.path.join(os.path.dirname(demo_file), "sample_lists", "ukb-sample-list.txt"), index=False)

    # # UkbDatset 클래스 초기화에서 제외할 subject IDs 명시적으로 지정
    # opt_outs = os.path.join(os.path.dirname(demo_file), "ukb-opt-outs.csv")
    # subject_ids = df.columns.drop('eid')
    # subject_ids = pd.DataFrame(subject_ids, columns=['eid'])
    # subject_ids.to_csv(opt_outs, index=False)
    # opt_outs = os.path.join(os.path.dirname(demo_file), "ukb-opt-outs.csv")
    # pd.DataFrame(df['eid'].unique()).to_csv(opt_outs, index=False)

    return df, output_dir


def prepare_genomic_data(genome_base_path):
    """
    유전체 데이터(.bed, .fam, .bim)를 확인하고 필요한 전처리를 수행합니다.
    
    Args:
        genome_base_path: 유전체 데이터 파일의 경로 (디렉토리 경로)
    
    Returns:
        유전체 데이터 파일 경로 딕셔너리
    """
    print(f"유전체 데이터 디렉토리 확인 중: {genome_base_path}")
    
    # 필요한 파일들이 있는지 확인
    required_extensions = ['.bed', '.fam', '.bim']
    genome_files = {}
    
    # 디렉토리에서 파일 찾기
    files_in_dir = os.listdir(genome_base_path)
    base_file_found = False
    
    # 확장자가 있는 파일 중에서 공통된 베이스 이름 찾기
    for file in files_in_dir:
        if file.endswith('.bed'):
            base_name = file[:-4] # .bed 확장자 제거
            base_file_found = True
            break
    
    if not base_file_found:
        raise FileNotFoundError(f"유전체 데이터 파일(.bed)을 찾을 수 없습니다: {genome_base_path}")
    
    # 베이스 이름을 사용하여 모든 필요한 파일 확인
    for ext in required_extensions:
        file_name = f"{base_name}{ext}"
        file_path = os.path.join(genome_base_path, file_name)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"유전체 데이터 파일을 찾을 수 없습니다: {file_path}")
        genome_files[ext[1:]] = file_path
    
    print(f"모든 유전체 데이터 파일을 찾았습니다.")
    return genome_files

def make_v16_config(*, ancestry=None, ctl_ratio=19, time_model=ModelType.STANDARD, 
                   feature_set='67bm', feature_selection_iterations=None,
                   ukb_data_location=None):
    """
    MILTON 분석을 위한 설정을 생성합니다.
    
    Args:
        ancestry: 인종별 샘플 ID가 포함된 파일 경로 (튜플: (qv_model_dir, qv_subject_subset))
        ctl_ratio: 케이스 대비 컨트롤 비율
        time_model: 시간 모델 유형 (STANDARD, PROGNOSTIC, DIAGNOSTIC)
        feature_set: 특성 집합 ('67bm', 'olink-only', 'olink-and-67bm')
        feature_selection_iterations: 특성 선택을 위한 반복 횟수
        ukb_data_location: UKB 데이터 위치
    
    Returns:
        MILTON 분석 설정
    """
    conf = Settings()
    
    # UKB 데이터 위치 설정
    if ukb_data_location:
        conf().dataset.location = ukb_data_location
    
    if ancestry:
        # 특정 인종 설정 사용
        qv_model_dir, qv_subject_subset = ancestry
        conf().analysis.qv_model_dir = qv_model_dir
        conf().analysis.qv_subject_subset = qv_subject_subset
        # 특정 인종은 훈련도 제한됨
        ids = pd.read_csv(qv_subject_subset, usecols=['eid'])['eid']
        conf().patients.used_subjects = ids.to_list()
        
    if ctl_ratio:
        conf().patients.controls_to_cases_ratio = ctl_ratio
        
    if feature_selection_iterations is None:
        feature_selection_iterations = ctl_ratio
    
    # 특성 집합 설정
    if feature_set == '67bm': # 67 바이오마커만
        conf.features.biomarkers = True
        conf.features.respiratory = True
        conf.features.overall_health = True
        conf.features.olink = False
        conf.features.olink_covariates = False
        
    elif feature_set == 'olink-only': # olink 단백질체만
        conf.features.biomarkers = False
        conf.features.olink = True
        conf.features.olink_covariates = True

        conf().feature_selection.iterations = feature_selection_iterations
        conf().feature_selection.preserved = [ # 모든 공변량
            Col.AGE, 
            Col.GENDER, 
            'Alcohol intake frequency.',
            'Illnesses of father',
            'Illnesses of mother',
            'Smoking status',
            'Blood-type haplotype',
            'Body mass index (BMI)'
        ]
    
    elif feature_set == 'olink-and-67bm': # olink와 67 바이오마커
        conf.features.biomarkers = True
        conf.features.respiratory = True
        conf.features.overall_health = True
        conf.features.olink = True
        conf.features.olink_covariates = True

        conf().feature_selection.iterations = feature_selection_iterations # 특성 선택을 위한 반복 횟수
        conf().feature_selection.preserved = [ # 모든 공변량
            Col.AGE, 
            Col.GENDER, 
            'Alcohol intake frequency.',
            'Illnesses of father',
            'Illnesses of mother',
            'Smoking status',
            'Blood-type haplotype',
            'Body mass index (BMI)'
        ]
        # 특성 선택에서 67개 특성을 제외하고 olink 단백질만 사용
        ukb_biomarkers = DD.predefined(biomarkers=True, respiratory=True, overall_health=True)\
        .index.drop_duplicates()\
        .drop([Col.GENDER, Col.AGE])\
        .to_list()
        conf().feature_selection.preserved.extend(ukb_biomarkers)
    
    else:
        print('특성 집합이 정의되지 않았습니다. 67개 바이오마커로 진행합니다...')
        conf.features.biomarkers = True
        conf.features.respiratory = True
        conf.features.overall_health = True
        conf.features.olink = False
        conf.features.olink_covariates = False
    
    # NA 데이터 처리 전략
    conf().preproc.na_imputation = 'median'
    conf().preproc.na_imputation_extra = {
        'Testosterone': GenderSpecNAStrategy(males='median', females='median'),
        Col.RHEUMATOID_FACTOR: ('constant', 0.0),
        Col.OESTRADIOL: GenderSpecNAStrategy(males=36.71, females=110.13),
    }

    # 분석 설정
    conf().analysis.default_model = 'xgb'
    conf().analysis.hyper_parameters = {
        'n_estimators': [50, 100, 200, 300],
    }
    conf().analysis.hyper_param_metric = 'roc_auc' 
    conf().analysis.n_replicas = 10 # XGBoost 훈련을 위한 복제본 수
    conf().analysis.evaluate_all_replicas = True
    
    conf().analysis.model_type = time_model
    return conf

def run_milton_analysis(settings, out_dir, code=None):
    """
    MILTON 분석을 실행하고 결과를 저장합니다.
    
    Args:
        settings: MILTON 설정
        out_dir: 결과를 저장할 디렉토리
        code: 분석할 ICD10 코드
    """
    # ICD10 코드가 제공된 경우 해당 코드에 대한 케이스와 컨트롤 설정
    if code:
        settings().patients.spec = ICD10.find_by_code(code)
    
    # 분석 실행
    print("MILTON 분석을 시작합니다...")
    ev = Evaluator(settings)
    ev.run()
    
    # 결과 저장
    print(f"분석 결과를 저장합니다: {out_dir}")
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    ev.save_report(out_dir)
    print("분석이 완료되었습니다.")

def main():

    print("MILTON 분석 시작")
    
    # 역학 데이터 준비
    if DEMO_DATA_PATH.endswith('.parquet'):
        demo_df = pd.read_parquet(DEMO_DATA_PATH)
        demo_processed_path = DEMO_DATA_PATH
    else:
        demo_df, demo_processed_dir = prepare_demographic_data(DEMO_DATA_PATH)
    
    # 유전체 데이터 준비
    genome_files = prepare_genomic_data(GENOME_DATA_PATH)
    
    # MILTON 세션 시작
    print("MILTON 세션을 시작합니다...")
    sess = Session('local', 
                    n_workers=4, # 로컬 머신 코어 수에 맞게 조정
                    memory='4G',
                    data_location=os.path.dirname(demo_processed_dir),
                    caching=False)
    
    # 분석 설정
    ctl_ratio = 9 # 케이스 대비 컨트롤 비율
    time_model = 'time_agnostic' # 시간 모델 유형 ('time_agnostic', 'prognostic', 'diagnostic')
    # ancestry_name = 'EAS' # 동아시아 인종 (AFR, AMR, EAS, EUR, SAS 중 하나)
    feature_set = '67bm' # 특성 집합 ('67bm', 'olink-only', 'olink-and-67bm')
    
    # 분석할 ICD10 코드 (질병 코드)
    code = 'E11' # 당뇨병 관련 코드 (예시)
    
    # 결과 디렉토리 설정
    out_dir = os.path.join(OUTPUT_BASE_PATH, code, feature_set, time_model)
    
    # 시간 모델 설정
    if time_model == 'time_agnostic':
        timemodel = ModelType.STANDARD
    elif time_model == 'prognostic':
        timemodel = ModelType.PROGNOSTIC
    elif time_model == 'diagnostic':
        timemodel = ModelType.DIAGNOSTIC
    else:
        print('time_model이 정의되지 않았습니다')
        timemodel = ModelType.STANDARD
    
    # 인종 관련 설정 (필요한 경우 활성화)
    ancestry_data = None
    # 실제로 인종 관련 데이터를 사용하려면 아래 주석을 해제하고 경로 지정
    # qv_model_dir = "/path/to/qv_model_dir"
    # qv_subject_subset = f"/path/to/ancestry/{ancestry_name}_subjects.csv"
    # ancestry_data = (qv_model_dir, qv_subject_subset)
    
    # 설정 생성
    settings = make_v16_config(
        ancestry=ancestry_data,
        ctl_ratio=ctl_ratio,
        time_model=timemodel,
        feature_set=feature_set,
        ukb_data_location=os.path.dirname(demo_processed_dir)
    )

    # ancestry 관련 자동 설정 방지 - 명시적으로 비활성화
    settings().analysis.qv_model_dir = None
    settings().analysis.qv_subject_subset = None
    settings().patients.used_subjects = None
    
    # 최소 케이스 수 설정
    settings().analysis.min_cases = 0
    
    # 설정 출력
    print("MILTON 분석 설정:")
    print(settings())
    
    # MILTON 분석 실행
    run_milton_analysis(settings, out_dir, code)

    print("MILTON 분석 종료")

if __name__ == "__main__":
    main() 