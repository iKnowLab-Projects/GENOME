# GENOME
[milton](https://github.com/astrazeneca-cgr-publications/milton-release) 코드를 활용한 유전체 및 역학 데이터 기반 질병 예측

## 설치

```
git clone https://github.com/iKnowLab-Projects/GENOME.git
cd GENOME/code/milton-release
```

MILTON is not currently available via PyPi but it will install with pip in debug 
mode. For complete installation procedure that creates a conda env and installs
everything with pip, run:
```
scripts/init.sh
```
If you already have a conda env that you want to use, activate it and from
within the MILTON folder run:
```
pip install -r requirements.txt
pip install -e .
```

## Data Structure

```
├── UKB_genome/         # 유전체 데이터
│   ├── UKB_genome.bed
│   ├── UKB_genome.bim
│   └── UKB_genome.fam
├── total_demo.txt      # 역학 데이터
```

## Usage

### Modify absolute data paths in file

```
# code/milton-release/process_ukb_data.py
GENOME_DATA_PATH = "/home/soyeon/workspace/data/GENOME/UKB_genome"  # .bed, .fam, .bim 파일이 있는 경로
DEMO_DATA_PATH = "/home/soyeon/workspace/data/GENOME/total_demo.txt"  # 역학 데이터 경로    # 확장자: .txt 혹은 .parquet
OUTPUT_BASE_PATH = "./results"  # 결과 저장 경로
```

#### (Optional) Data exploration: `eda.ipynb`


### Run analysis code

```
python process_ukb_data.py
```
