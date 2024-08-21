lake_path = {
    "landing_adventure_works": "s3a://landing/adventure_works/",
    "bronze": "s3a://bronze/adventure_works/",
    "silver": "s3a://silver/adventure_works/",
    "gold": "s3a://gold/adventure_works/",
}

prefix_layer_name = {"0": "landing_", "1": "bronze_", "2": "silver_", "3": "gold_"}

# ************************
# Start Bronze Tables
# ************************
tables_postgres_adventureworks = {
    "1": "sales.countryregioncurrency",
    "2": "sales.creditcard",
    "3": "sales.currency",
    #'4': 'sales.currencyrate',
    #'5': 'sales.customer',
    #'6': 'sales.personcreditcard',
    #'7': 'sales.salesorderdetail',
    #'8': 'sales.salestaxrate',
    #'9': 'sales.salesorderheadersalesreason',
    #'10': 'sales.salesperson',
    #'11': 'sales.salespersonquotahistory',
    #'12': 'sales.salesreason',
    #'13': 'sales.salestaxrate',
    #'14': 'sales.salesterritory',
    #'15': 'sales.salesterritoryhistory',
    #'16': 'sales.shoppingcartitem',
    #'17': 'sales.specialoffer',
    #'18': 'sales.specialofferproduct',
    #'19': 'sales.store',
    "20": "humanresources.department",
    "21": "humanresources.employee",
    "22": "sales.salesorderheader",
}

# ************************
# Start Silver Tables
# ************************
tables_silver = {
    "sales_countryregioncurrency": f"""
SELECT
	countryregioncode as codigo_pais,
	currencycode as codigo_moeda,
	modifieddate,
    month_key
FROM
    delta.`{{hdfs_source}}{{prefix_layer_name_source}}sales_countryregioncurrency`
""",
    "humanresources_department": f"""SELECT * FROM delta.`{{hdfs_source}}{{prefix_layer_name_source}}humanresources_department`""",
    "humanresources_employee": f"""SELECT * FROM delta.`{{hdfs_source}}{{prefix_layer_name_source}}humanresources_employee`""",
    "sales_salesorderheader": f"""SELECT * FROM delta.`{{hdfs_source}}{{prefix_layer_name_source}}sales_salesorderheader`""",
}

# ************************
# Start Gold Tables
# ************************
tables_gold = {
    # Humanresources Department
    "humanresources_department": """
SELECT
    departmentid as id_departamento,
    name as nome_departamento,
    groupname as nome_grupo,
    modifieddate,
    last_update,
    month_key
FROM
    delta.`s3a://silver/adventure_works/silver_humanresources_department`
    """,
    # Qtd Humanresources Group Name
    "humanresources_groupname_qtd": """
SELECT
    groupname as nome_grupo,
    modifieddate,
    last_update,
    month_key,
    count(*) as qtd
FROM
    delta.`s3a://silver/adventure_works/silver_humanresources_department`
group by
	groupname,
	modifieddate,
    last_update,
    month_key
    """,
}
