CREATE TABLE products(
            id varchar,
            name varchar,
            price int,
            category_id int,
            discount_id int,
            brand_id int,
            variant_id int,
            doelgroep_id int,
            gender_id int
        );
        CREATE TABLE categories(
            id int,
            category varchar,
            sub_category varchar,
            sub_sub_category varchar
        );
        CREATE TABLE discounts(
            id int,
            discount varchar
        );
        CREATE TABLE brands(
            id int,
            brand varchar
        );
        CREATE TABLE variants(
            id int,
            variant varchar
        );
        CREATE TABLE doelgroepen(
            id int,
            doelgroep varchar
        );
        CREATE TABLE genders(
            id int,
            gender varchar
        );
        CREATE TABLE sessions(
            id varchar,
            session_start date,
            session_end date,
            buid varchar,
            has_sale boolean
        );
        CREATE TABLE profiles(
            id varchar,
            buids text[],
            previously_recommended text[],
            segment varchar,
            viewed_before text[],
            similars text[]
        );
        CREATE TABLE prosess(
	        profile_id varchar,
	        session_id varchar,
	        buid varchar
        );