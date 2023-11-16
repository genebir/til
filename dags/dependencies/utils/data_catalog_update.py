import boto3
import pandas as pd

def data_catalog_update(df: pd.DataFrame) -> None:
    """
    Glue Data Catalog의 Table 및 Column Comment를 업데이트하는 함수
    :param df: DataFrame
    :return: None

    """
    # Glue client 생성
    glue_client = boto3.client('glue', region_name='ap-northeast-2')
    # DB_NAME, TABLE_NAME, TABLE_COMMENT 컬럼을 기준으로 중복 제거
    meta_info_key = df[['DB_NAME', 'TABLE_NAME', 'TABLE_COMMENT']].drop_duplicates().to_dict('records')
    print(meta_info_key)
    # 중복 제거된 meta_info_key를 기준으로 Glue Data Catalog의 Table/Column Comment를 업데이트
    for meta_info in meta_info_key:
        try:
            # Glue Data Catalog의 Table 정보를 가져옴
            table_info = glue_client.get_table(
                DatabaseName=meta_info['DB_NAME'],
                Name=meta_info['TABLE_NAME']
            )
            print(table_info)
            # column 정보를 가져옴
            columns = table_info['Table']['StorageDescriptor']['Columns']
            # column 정보에 Comment를 추가
            for i in range(len(columns)):
                try:
                    columns[i]['Comment'] = df.loc[(df.DB_NAME == meta_info['DB_NAME']) &
                                                   (df.TABLE_NAME == meta_info['TABLE_NAME']) &
                                                   (df.COLUMN_NAME == columns[i]['Name'])]['COLUMN_COMMENT'].values[0]
                except Exception as e:
                    # column 정보에 Comment가 없다면 빈 값으로 처리
                    columns[i]['Comment'] = ''
                    continue
            # 변경된 column 정보를 table 정보에 업데이트
            table_info['Table']['StorageDescriptor']['Columns'] = columns
            # table 정보에 Description를 추가
            table_info['Table']['Description'] = meta_info['TABLE_COMMENT']
            print(table_info)
            # 변경된 table 정보를 Glue Data Catalog에 업데이트
            response = glue_client.update_table(
                DatabaseName=table_info['Table']['DatabaseName'],
                TableInput={
                    'Name': table_info['Table']['Name'],
                    'Description': table_info['Table']['Description'],
                    'StorageDescriptor': table_info['Table']['StorageDescriptor'],
                    'PartitionKeys': table_info['Table']['PartitionKeys'],
                    'TableType': table_info['Table']['TableType'],
                    'Parameters': table_info['Table']['Parameters']
                }
            )

            print(f"Updated {meta_info['TABLE_NAME']} in {meta_info['DB_NAME']}")
        except Exception as e:
            print(e)
            continue