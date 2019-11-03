class SqlQueries:
    create_table = {}
    load_table = {}
    copy_from_s3 = {}
    
    # stage tables
    create_table['stage_immigraton'] = """
        CREATE TABLE IF Not Exists stage_immigraton
        (
            cicid	float,
            i94yr	float,
            i94mon	float,
            i94cit	float,
            i94res	float,
            i94port	varchar(200),
            arrdate	float,
            i94mode	float,
            i94addr	varchar(200),
            depdate	float,
            i94bir	float,
            i94visa	float,
            count	float,
            dtadfile	varchar(200),
            visapost	varchar(200),
            occup	varchar(200),
            entdepa	varchar(200),
            entdepd	varchar(200),
            entdepu	varchar(200),
            matflag	varchar(200),
            biryear	float,
            dtaddto	varchar(200),
            gender	varchar(200),
            insnum	varchar(200),
            airline	varchar(200),
            admnum	float,
            fltno	varchar(200),
            visatype	varchar(200)
        )
    """

    create_table['stage_i94prtl'] = """
        Create Table IF Not Exists stage_i94prtl
        (
            Code varchar(200),
            Name varchar(800)
        )
        """
    
    create_table['stage_i94cntyl'] = """
        Create Table IF Not Exists stage_i94cntyl
        (
            Code varchar(200),
            Name varchar(800)
        )
        """

    create_table['stage_i94visa'] = """
        Create Table IF Not Exists stage_i94visa
        (
            Code varchar(200),
            Name varchar(800)
        )
    """

    create_table['stage_airport_codes'] = """
        CREATE TABLE IF NOT EXISTS stage_airport_codes (
    		ident varchar(200),
    		type varchar(200),
    		name varchar(800),
    		elevation_ft float,
    		continent varchar(200),
    		iso_country varchar(200),
    		iso_region varchar(200),
    		municipality varchar(200),
    		gps_code varchar(200),
    		iata_code varchar(200),
    		local_code varchar(200),
    		coordinates varchar(200),
    		longitude float,
    		latitude float
        )
    """

    create_table['stage_us_cities_demographics'] = """
    	CREATE TABLE IF NOT EXISTS stage_us_cities_demographics (
    		city varchar(200),
    		state varchar(200),
    		median_age float,
    		male_population float,
    		female_population float,
    		total_population float,
    		number_of_veterans float,
    		foreign_born float,
    		average_household_size float,
    		state_code varchar(200),
    		race varchar(200),
    		count int
        )
    """

    create_table['stage_iso_country_region'] = """
        CREATE TABLE stage_iso_country_region(
            country_code  varchar(500),
            country_name  varchar(500), 
            region_code  varchar(500),
            region_name  varchar(500)
        )
    """

    # formal data table
    create_table['immigraton'] = """
        CREATE TABLE IF Not Exists immigraton
        (
            ccid  int,
            reg_year  int,
            reg_month  int,
            orgin_country_code  int,
            port_of_entry_code  varchar(200),
            arrival_date  date,
            state_code  varchar(200),
            depart_date  date,
            age  int,
            visa_type  int,
            reg_date  date,
            gender  varchar(200),
            airline  varchar(200),
            admnum  varchar(200),
            flight_no  varchar(200)
        )
        DISTKEY(reg_year)
        SORTKEY(reg_year,reg_month)
    """

    create_table ['country'] = """
        Create Table IF Not Exists country
        (
            country_code varchar(200),
            country_name varchar(800)
        )
        """
    
    create_table['port_of_entry'] = """
        Create Table IF Not Exists port_of_entry
        (
            port_code varchar(200),
            city varchar(800),
            state_or_country varchar(200)
        )
        """

    create_table['visa_type'] = """
        Create Table IF Not Exists visa_type
        (
            id int,
            type varchar(800)
        )
    """

    create_table['airport_codes'] = """
        CREATE TABLE IF NOT EXISTS airport_codes (
    		ident varchar(200),
    		type varchar(200),
    		name varchar(800),
    		elevation_ft float,
    		continent varchar(200),
    		iso_country varchar(200),
            iso_country_name varchar(200),
    		iso_region varchar(200),
    		iso_region_name varchar(200),
    		municipality varchar(200),
    		gps_code varchar(200),
    		iata_code varchar(200),
    		local_code varchar(200),
    		coordinates varchar(200),
    		longitude float,
    		latitude float
        )
    """

    create_table['us_cities_demographics'] = """
    	CREATE TABLE IF NOT EXISTS us_cities_demographics (
    		city varchar(200),
    		state varchar(200),
    		median_age float,
    		male_population int,
    		female_population int,
    		total_population int,
    		number_of_veterans int,
    		foreign_born int,
    		average_household_size float,
    		state_code varchar(200),
    		race varchar(200),
    		count int
        )
    """

    create_table['iso_country_region'] = """
            CREATE TABLE iso_country_region(
                country_code  varchar(500),
                country_name  varchar(500), 
                region_code  varchar(500),
                region_name  varchar(500)
            )
        """
    
    # load data from stage
    load_table['immigraton'] = """
        BEGIN;

            DELETE
            FROM immigraton
            WHERE ccid IN (SELECT ccid FROM stage_immigraton);

            INSERT INTO immigraton
            (
            ccid
            ,reg_year
            ,reg_month
            ,orgin_country_code
            ,port_of_entry_code
            ,arrival_date
            ,state_code
            ,depart_date
            ,AGE
            ,visa_type
            ,reg_date
            ,gender
            ,airline
            ,admnum
            ,flight_no
            )
            SELECT cicid
                ,i94yr
                ,i94mon
                ,i94cit
                ,i94port
                ,dateadd(day,CAST(arrdate AS INT),'19600101')
                ,i94addr
                ,dateadd(day,CAST(depdate AS INT),'19600101')
                ,i94bir
                ,i94visa
                ,TO_DATE(dtadfile,'YYYYMMDD')
                ,gender
                ,airline
                ,CAST(admnum AS VARCHAR(200))
                ,fltno
            FROM stage_immigraton;

        COMMIT;
    """

    load_table['airport_codes'] = """
        BEGIN;
            TRUNCATE TABLE airport_codes;
            INSERT INTO airport_codes
            (
            ident
            ,TYPE
            ,name
            ,elevation_ft
            ,continent
            ,iso_country
            ,iso_region
            ,municipality
            ,gps_code
            ,iata_code
            ,local_code
            ,coordinates
            ,longitude
            ,latitude
            )
            SELECT ident
                ,TYPE
                ,name
                ,elevation_ft
                ,continent
                ,iso_country
                ,iso_region
                ,municipality
                ,gps_code
                ,iata_code
                ,local_code
                ,coordinates
                ,longitude
                ,latitude
            FROM stage_airport_codes;
        COMMIT;
    """
    
    load_table['visa_type'] = """
            BEGIN;
                TRUNCATE TABLE visa_type;
                INSERT INTO visa_type(id,type)
                    SELECT CAST(code AS INT),name FROM stage_i94visa;
            COMMIT;
        """

    load_table['country'] = """
        BEGIN;
            TRUNCATE TABLE country;;
            INSERT INTO country(country_code,country_name)
                SELECT code,name FROM stage_i94cntyl;
        COMMIT;
    """

    load_table['port_of_entry'] = """
        BEGIN;
            TRUNCATE TABLE port_of_entry;
            INSERT INTO port_of_entry(port_code,city,state_or_country)
                SELECT code,split_part(name,',',1),split_part(name,',',2)
                FROM stage_i94prtl;
        COMMIT;
    """

    load_table['iso_country_region'] = """
        BEGIN;
            TRUNCATE TABLE iso_country_region;
            INSERT INTO iso_country_region(country_code,country_name,region_code,region_name)
                SELECT country_code,country_name,region_code,region_name 
                FROM stage_iso_country_region;
        COMMIT;
    """

    load_table['us_cities_demographics'] = """
        BEGIN;
            TRUNCATE TABLE us_cities_demographics;
            INSERT INTO us_cities_demographics
            (
            city
            ,state
            ,median_age
            ,male_population
            ,female_population
            ,total_population
            ,number_of_veterans
            ,foreign_born
            ,average_household_size
            ,state_code
            ,race
            ,COUNT
            )
            SELECT city
                ,state
                ,median_age
                ,male_population
                ,female_population
                ,total_population
                ,number_of_veterans
                ,foreign_born
                ,average_household_size
                ,state_code
                ,race
                ,COUNT
            FROM stage_us_cities_demographics;
        COMMIT;
    """

    copy_from_s3['csv'] = """
        COPY {table} 
        FROM '{s3_path}'
        IGNOREHEADER {ignore_header}
        DELIMITER '{delimiter}'
        IAM_ROLE '{iam_role}'
        REGION '{region}'
        CSV
    """

    copy_from_s3['parquet'] = """
        COPY {table} 
        FROM '{s3_path}'
        IAM_ROLE '{iam_role}'
        FORMAT AS PARQUET
    """
    
    check_sql = """
        SELECT COUNT(*) FROM {table}
    """
