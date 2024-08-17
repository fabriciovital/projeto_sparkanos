lake_path = {
    "landing_adventure_works": "s3a://landing/adventure_works/",
    "bronze": "s3a://bronze/adventure_works/",
    "silver": "s3a://silver/adventure_works/",
    "gold": "s3a://gold/adventure_works/",
}

prefix_layer_name = {"1": "bronze_", "2": "silver_", "3": "gold_"}

### Start Bronze Tables ###
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
