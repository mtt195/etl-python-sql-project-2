# -------------------------------------------------- #
# AUTHOR NAME: MACIEJ TOMASZEWSKI                    #
# CREATE DATE: 05.03.2024                            #
# DESCRIPTION:                                       #
#  MY SECOND PYTHON & SQL PROJECT. THIS SCRIPT IS    #
#  USED TO PROCESS SAMPLE, FICTIOUS DATA USING       #
#  PYTHON: EXTRACT FROM MULTIPLE SOURCES, TRANSFORM  #
#  AND FINALLY LOAD TO DEDICATED TABLES IN GIVEN     #
#  DATABASE. THIS IS THE ONLY PYTHON SCRIPT USED.    #

#                PYTHON SCRIPT 1/1                   #
# -------------------------------------------------- #
v_step, v_status = '0 import libraries and set variables', 'ERROR' # FIRST STEP

#################
try:
#################
  from sqlalchemy import create_engine, MetaData, text
  from sqlalchemy import Table, Column, String, Integer, Numeric, TIMESTAMP, Date, Sequence
  from sqlalchemy import Insert, Update, Delete, Select, between, join, literal_column, case, func

  v_added_by = 'YourName.YourSurname@YourCompany.com' # PLEASE CHANGE BEFORE CODE EXECUTION!

  # connect oracle
  v_user_oracle = 'system'      # PLEASE CHANGE BEFORE CODE EXECUTION!
  v_password_oracle = 'oracle'  # PLEASE CHANGE BEFORE CODE EXECUTION!
  v_host_oracle = 'localhost'   # PLEASE CHANGE BEFORE CODE EXECUTION!
  v_port_oracle = '1521'        # PLEASE CHANGE BEFORE CODE EXECUTION!
  v_host_mssql = 'localhost'    # PLEASE CHANGE BEFORE CODE EXECUTION!

  v_eng = create_engine(f'oracle://{v_user_oracle}:{v_password_oracle}@{v_host_oracle}:{v_port_oracle}/xe')
  v_md = MetaData()

  # files path
  v_all_fpath = r'C:\Users\tomas\Desktop\Python\Projects\ETL 2\3_src_data\3_2 external files\3_2_' # PLEASE CHANGE BEFORE CODE EXECUTION!

  v_avr_fpath = v_all_fpath + 'hr_avro.avro'
  v_jsn_fpath = v_all_fpath + 'hr_json.json'
  v_prq_fpath = v_all_fpath + 'hr_parquet.parquet'
  v_xml_fpath = v_all_fpath + 'hr_xml.xml'
  v_exc_fpath = v_all_fpath + 'hr_excel.xlsx'

  # stage tables
  ########################################################################
  v_step = '1 create stage tables' # NEXT STEP
  print(v_step, 'starts...')

  t_avr_stg_nm, t_jsn_stg_nm, t_prq_stg_nm, t_xml_stg_nm = 'hr_avro_stage', 'hr_json_stage', 'hr_parquet_stage', 'hr_xml_stage'
  t_exc_stg_nm, t_msql_stg_nm = 'hr_excel_stage', 'hr_mssql_stage'

  v_con = v_eng.connect()
  v_con.begin()

  # create
  ####################################
  # avro
  t_hr_avr_stg = Table(t_avr_stg_nm, v_md
      ,Column('lp', Integer)
      ,Column('fname', String(50))
      ,Column('lname', String(50))
      ,Column('gender', String(1))
      ,Column('date_of_birth', String(50))
      ,Column('personal_id', Integer)
      ,Column('id_card_number', String(7))
      ,Column('country', String(50))
      ,Column('city', String(50))
    )

  # json
  t_hr_jsn_stg = Table(t_jsn_stg_nm, v_md
      ,Column('fname', String(50))
      ,Column('lname', String(50))
      ,Column('gender', String(1))
      ,Column('date_of_birth', String(50))
      ,Column('personal_id', Integer)
      ,Column('id_card_number', String(7))
      ,Column('city', String(50))
    )

  # parquet
  t_hr_prq_stg = Table(t_prq_stg_nm, v_md
      ,Column('lp', Integer)
      ,Column('fullname', String(50))
      ,Column('gender', String(1))
      ,Column('date_of_birth', String(50))
      ,Column('personal_id', Integer)
      ,Column('id_card_number', String(7))
      ,Column('country', String(50))
      ,Column('city', String(50))
    )

  # xml
  t_hr_xml_stg = Table(t_xml_stg_nm, v_md
      ,Column('fname', String(50))
      ,Column('lname', String(50))
      ,Column('gender', String(1))
      ,Column('date_of_birth', String(50))
      ,Column('personal_id', Integer)
      ,Column('id_card_number', String(7))
      ,Column('city', String(50))
    )

  # excel
  t_hr_exc_stg = Table(t_exc_stg_nm, v_md
      ,Column('lp', Integer)
      ,Column('fname', String(50))
      ,Column('lname', String(50))
      ,Column('gender', String(1))
      ,Column('date_of_birth', String(50))
      ,Column('personal_id', Integer)
      ,Column('id_card_number', String(7))
      ,Column('country', String(50))
      ,Column('city', String(50))
    )

  # mssql
  t_hr_msql_stg = Table(t_msql_stg_nm, v_md
      ,Column('lp', Integer)
      ,Column('fname', String(50))
      ,Column('lname', String(50))
      ,Column('gender', String(1))
      ,Column('date_of_birth', String(50))
      ,Column('personal_id', Integer)
      ,Column('id_card_number', String(7))
      ,Column('country', String(50))
      ,Column('city', String(50))
    )

  v_md.create_all(v_eng)

  # insert
  ####################################
  print(v_step, 'completed successfully')
  v_step = '2 prepare and insert data to stage tables' # NEXT STEP
  print(v_step, 'starts...')

  # avro
  #################
  import cx_Oracle, fastavro

  # open file and import data
  with open(v_avr_fpath, 'rb') as avro_file:
    avro_reader = fastavro.reader(avro_file)

    # prepare columns and values to insert
    column_names = [field['name'] for field in avro_reader.writer_schema['fields']]
    columns_str = ','.join(column_names)
    values_str = ','.join(f':{name}' for name in column_names)

    insert_query = text(f'INSERT INTO {t_avr_stg_nm} ({columns_str}) VALUES ({values_str})')
    data = list(avro_reader)
    v_con.execute(insert_query, data)

  # json
  #################
  import json

  # open file and import data
  with open(v_jsn_fpath, 'r', encoding='utf-8') as json_file:
    json_data = json_file.read()

    # prepare columns and values to insert
    columns_list = [column.name for column in t_hr_jsn_stg.columns]
    columns_str = ','.join(column for column in columns_list)
    values_str = ','.join(f':{name}' for name in columns_list)

    insert_query = text(f'INSERT INTO {t_jsn_stg_nm} ({columns_str}) VALUES ({values_str})')
    data = json.loads(json_data)
    v_con.execute(insert_query, data)

  # parquet
  #################
  import pyarrow.parquet as pq

  # open file and import data
  table = pq.read_table(v_prq_fpath)
  df = table.to_pandas()

  # prepare columns and values to insert
  columns_list = list(col for col in df.columns)
  columns_str = ','.join(col for col in columns_list)
  values_str = ','.join(f':{col}' for col in columns_list)

  insert_query = text(f'INSERT INTO {t_prq_stg_nm} ({columns_str}) VALUES ({values_str})')
  data = [dict(zip(df.columns, row)) for row in df.itertuples(index=False, name=None)]
  v_con.execute(insert_query, data)

  # xml
  #################
  import xmltodict

  # open file and import data
  xml_file = open(v_xml_fpath, 'r', encoding='utf-8')
  xml_str = xml_file.read()
  data_stage = xmltodict.parse(xml_str)
  data = data_stage['root']['row']

  # prepare columns and values to insert
  columns_list = [col.name for col in t_hr_xml_stg.columns]
  columns_str = ','.join(col for col in columns_list)
  values_str = ','.join(f':{col}' for col in columns_list)

  # data load
  insert_query = text(f'INSERT INTO {t_xml_stg_nm} ({columns_str}) VALUES ({values_str})')
  v_con.execute(insert_query, data)

  # excel
  #################
  from pandas import ExcelFile

  # open file and import data
  v_xlsx = ExcelFile(v_exc_fpath)
  data_stage = v_xlsx.parse(v_xlsx.sheet_names[0])
  data = data_stage.iloc[0].to_dict()

  # prepare columns and values to insert
  columns_list = [col.name for col in t_hr_exc_stg.columns]
  columns_str = ','.join(col for col in columns_list)
  values_str = ','.join(f':{col}' for col in columns_list)

  # data load
  insert_query = text(f'INSERT INTO {t_exc_stg_nm} ({columns_str}) VALUES ({values_str})')
  v_con.execute(insert_query, data)

  # ms sql
  #################
  # connect
  v_eng_msql = create_engine(f'mssql://@{v_host_mssql}/MT_PythonSQL_Project2?driver=odbc+driver+17+for+sql+server')
  v_md_msql = MetaData()
  v_con_msql = v_eng_msql.connect()

  t_hr, t_hr_addr = Table('HR', v_md_msql, autoload_with = v_eng_msql), Table('HR_addr', v_md_msql, autoload_with = v_eng_msql)

  # get data
  v_query_mssql = Select(
    t_hr.c.lp
    ,t_hr.c.fname
    ,t_hr.c.lname
    ,t_hr.c.gender
    ,t_hr.c.date_of_birth
    ,t_hr_addr.c.personal_id
    ,t_hr_addr.c.id_card_number
    ,t_hr_addr.c.country
    ,t_hr_addr.c.city
  ).join(t_hr_addr, t_hr_addr.c.hr_id == t_hr.c.hr_id, isouter = True)

  data_stage = v_con_msql.execute(v_query_mssql).fetchall()
  columns_list = [col.name for col in t_hr_msql_stg.columns]
  data = [dict(zip(columns_list, row)) for row in data_stage]

  # insert oracle table
  columns_str = ','.join(col for col in columns_list)
  values_str = ','.join(f':{col}' for col in columns_list)

  insert_query = text(f'INSERT INTO {t_msql_stg_nm} ({columns_str}) VALUES ({values_str})')
  v_con.execute(insert_query, data)

  # final table
  ########################################################################
  print(v_step, 'completed successfully')
  v_step = '3 create target table' # NEXT STEP
  print(v_step, 'starts...')

  v_tgt_tbl_nm, v_tgt_tbl_seq_nm = 'hr_customers', 'hr_customers_seq'

  # create
  ####################################
  # drop table and sequence if exists
  if v_eng.dialect.has_table(v_con, v_tgt_tbl_nm):
      t_hr_cust = Table(v_tgt_tbl_nm, v_md, autoload_with = v_eng)
      t_hr_cust.drop(v_eng)

  if v_eng.dialect.has_sequence(v_con, v_tgt_tbl_seq_nm):
      seq_hr_customers = Sequence(v_tgt_tbl_seq_nm, v_md)
      seq_hr_customers.drop(v_eng)

  # (re)create sequence
  sql_create_sequence = text(f'CREATE SEQUENCE {v_tgt_tbl_seq_nm} START WITH 1 INCREMENT BY 1')
  v_con.execute(sql_create_sequence)

  # (re)create table
  t_hr_cust = Table(v_tgt_tbl_nm, v_md,
      Column('cust_id', Integer, primary_key = True, server_default = text(f'{v_tgt_tbl_seq_nm}.nextval')),
      Column('added_on', TIMESTAMP),
      Column('added_by', String(100)),
      Column('cust_name', String(100)),
      Column('gender', String(1)),
      Column('date_of_birth', Date),
      Column('personal_id', Integer),
      Column('id_card_number', String(7)),
      Column('country', String(50)),
      Column('city', String(50)),
      Column('src', String(10))
      ,extend_existing = True
    )
  v_md.create_all(bind = v_eng, tables = [t_hr_cust])

  # prepare data
  ####################################
  print(v_step, 'completed successfully')
  v_step = '4 prepare data for target table' # NEXT STEP
  print(v_step, 'starts...')

  # avro
  sql_avro_data = Select(
      func.current_timestamp().label('added_on'),
      literal_column("'" + v_added_by + "'").label('added_by'),
      (t_hr_avr_stg.c.fname + ' ' + t_hr_avr_stg.c.lname).label('cust_name'),
      t_hr_avr_stg.c.gender.label('gender'),
      func.to_date(
          # year
          func.substr(t_hr_avr_stg.c.date_of_birth, 7, 4) + '-' +
          # month
          func.substr(t_hr_avr_stg.c.date_of_birth, 4, 2) + '-' +
          # day
          func.substr(t_hr_avr_stg.c.date_of_birth, 1, 2)
          ,'YYYY-MM-DD'
      ).label('date_of_birth'),
      t_hr_avr_stg.c.personal_id.label('personal_id'),
      t_hr_avr_stg.c.id_card_number.label('id_card_number'),
      t_hr_avr_stg.c.country.label('country'),
      t_hr_avr_stg.c.city.label('city'),
      literal_column("'avro'").label('src')
    )

  # json
  # dict to map months
  dct_map_mth_jsn = {'sty':'01', 'lut':'02', 'mar':'03', 'kwi':'04', 'maj':'05', 'cze':'06', 'lip':'07', 'sie':'08', 'wrz':'09', 'paź':'10', 'lis':'11', 'gru':'12'}

  sql_json_data = Select(
      func.now().label('added_on'),
      literal_column("'" + v_added_by + "'").label('added_by'),
      (t_hr_jsn_stg.c.fname + ' ' + t_hr_jsn_stg.c.lname).label('cust_name'),
      t_hr_jsn_stg.c.gender.label('gender'),
      func.to_date(
          # year
          func.substr(t_hr_jsn_stg.c.date_of_birth, -4)
          # month
          + '-' +
          case(dct_map_mth_jsn, value = func.substr(t_hr_jsn_stg.c.date_of_birth, func.instr(t_hr_jsn_stg.c.date_of_birth, '-') + 1, 3))
          # day; extract and convert depending on position of '-'
          + '-' +
          case( # convert 1 to 01, 2 to 02 etc. depending on position of '-'
              {2:('0' + func.substr(t_hr_jsn_stg.c.date_of_birth, 1, 1)), 3:func.substr(t_hr_jsn_stg.c.date_of_birth, 1, 2)},
              value = func.instr(t_hr_jsn_stg.c.date_of_birth, '-')
          )
        ,'YYYY-MM-DD'
      ).label('date_of_birth'),
      t_hr_jsn_stg.c.personal_id.label('personal_id'),
      t_hr_jsn_stg.c.id_card_number.label('id_card_number'),
      literal_column("'Poland'").label('country'),
      t_hr_jsn_stg.c.city.label('city'),
      literal_column("'json'").label('src')
    )

  # parquet
  sql_prq_data = Select(
      func.now().label('added_on'),
      literal_column("'" + v_added_by + "'").label('added_by'),
      t_hr_prq_stg.c.fullname.label('cust_name'),
      t_hr_prq_stg.c.gender.label('gender'),
      func.to_date(t_hr_prq_stg.c.date_of_birth, 'DD-MM-YYYY').label('date_of_birth'),
      t_hr_prq_stg.c.personal_id.label('personal_id'),
      t_hr_prq_stg.c.id_card_number.label('id_card_number'),
      t_hr_prq_stg.c.country.label('country'),
      t_hr_prq_stg.c.city.label('city'),
      literal_column("'parquet'").label('src')
    )

  # xml
  # dict to map months
  dct_map_mth_xml = {'sty': 'jan', 'lut': 'feb', 'mar': 'mar', 'kwi': 'apr', 'maj': 'may', 'cze': 'jun', 'lip': 'jul', 'sie': 'aug', 'wrz': 'sep', 'paź': 'oct', 'lis': 'nov', 'gru': 'dec'}

  sql_xml_data = Select(
      func.now().label('added_on'),
      literal_column("'" + v_added_by + "'").label('added_by'),
      (t_hr_xml_stg.c.fname + ' ' + t_hr_xml_stg.c.lname).label('cust_name'),
      t_hr_xml_stg.c.gender.label('gender'),
      func.to_date(
          (# year
          func.substr(t_hr_xml_stg.c.date_of_birth, - 4)
          # month; extract depending on position of '-'
          + '-' +
          case(dct_map_mth_xml, value = func.substr(t_hr_xml_stg.c.date_of_birth, func.instr(t_hr_xml_stg.c.date_of_birth, '-') + 1, 3))
          # day; extract depending on position of '-'
          + '-' +
          func.substr(t_hr_xml_stg.c.date_of_birth, 1, func.instr(t_hr_xml_stg.c.date_of_birth, '-') - 1)
          )
          ,'YYYY-MM-DD'
      ).label('date_of_birth'),
      t_hr_xml_stg.c.personal_id.label('personal_id'),
      t_hr_xml_stg.c.id_card_number.label('id_card_number'),
      literal_column("'Poland'").label('country'),
      t_hr_xml_stg.c.city.label('city'),
      literal_column("'xml'").label('src')
    )

  # excel
  sql_exc_data = Select(
      func.now().label('added_on'),
      literal_column("'" + v_added_by + "'").label('added_by'),
      (t_hr_exc_stg.c.fname + ' ' + t_hr_exc_stg.c.lname).label('cust_name'),
      t_hr_exc_stg.c.gender.label('gender'),
      func.to_date(t_hr_exc_stg.c.date_of_birth, 'YYYY-MM-DD').label('date_of_birth'),
      t_hr_exc_stg.c.personal_id.label('personal_id'),
      t_hr_exc_stg.c.id_card_number.label('id_card_number'),
      t_hr_exc_stg.c.country.label('country'),
      t_hr_exc_stg.c.city.label('city'),
      literal_column("'excel'").label('src')
    )

  # mssql
  sql_msql_data = Select(
      func.now().label('added_on'),
      literal_column("'" + v_added_by + "'").label('added_by'),
      (t_hr_msql_stg.c.fname + ' ' + t_hr_msql_stg.c.lname).label('cust_name'),
      t_hr_msql_stg.c.gender.label('gender'),
      t_hr_msql_stg.c.date_of_birth.label('date_of_birth'),
      t_hr_msql_stg.c.personal_id.label('personal_id'),
      t_hr_msql_stg.c.id_card_number.label('id_card_number'),
      t_hr_msql_stg.c.country.label('country'),
      t_hr_msql_stg.c.city.label('city'),
      literal_column("'mssql'").label('src')
    )

  # insert data
  ####################################
  print(v_step, 'completed successfully')
  v_step = '5 insert data to target table' # NEXT STEP
  print(v_step, 'starts...')

  v_cols = [
      t_hr_cust.c.added_on,
      t_hr_cust.c.added_by,
      t_hr_cust.c.cust_name,
      t_hr_cust.c.gender,
      t_hr_cust.c.date_of_birth,
      t_hr_cust.c.personal_id,
      t_hr_cust.c.id_card_number,
      t_hr_cust.c.country,
      t_hr_cust.c.city,
      t_hr_cust.c.src
    ]

  v_con.execute(Insert(t_hr_cust).from_select(v_cols, sql_avro_data)) # avro
  v_con.execute(Insert(t_hr_cust).from_select(v_cols, sql_json_data)) # json
  v_con.execute(Insert(t_hr_cust).from_select(v_cols, sql_prq_data)) # parquet
  v_con.execute(Insert(t_hr_cust).from_select(v_cols, sql_xml_data)) # xml
  v_con.execute(Insert(t_hr_cust).from_select(v_cols, sql_exc_data)) # excel
  v_con.execute(Insert(t_hr_cust).from_select(v_cols, sql_msql_data)) # mssql

  # drop stage tables (no longer needed)
  print(v_step, 'completed successfully')
  v_step = '6 drop stage tables' # FINAL STEP
  print(v_step, 'starts...')

  v_md.drop_all(bind = v_eng, tables = [t_hr_avr_stg, t_hr_jsn_stg, t_hr_prq_stg, t_hr_xml_stg, t_hr_exc_stg, t_hr_msql_stg])

  # commit and close connection
  v_con.commit()
  v_con.close()
  v_status = 'SUCCESS'

  print(v_step, 'completed successfully\n')

#################
except Exception as e:
#################
  v_con.rollback()
  v_con.close()
  print(f'process stopped, error occured in step [{v_step}]: {type(e).__name__} - {e}', '\n')

#################
finally:
#################    
  print('process completed with status:', v_status)
