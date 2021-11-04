drop table fact_transactions;
drop table dim_cards_hist;
drop table dim_accounts_hist;
drop table dim_clients_hist;
drop table dim_terminals_hist;
drop table report;
drop table stg_transactions;
drop table meta_accounts;
drop table meta_cards;
drop table meta_clients;
drop table meta_terminals;
drop table meta_transactions;
drop table meta_report;


create table dim_terminals_hist
(
	id serial,
    terminal_id varchar,
    terminal_type varchar not null,
    terminal_city varchar not null,
    terminal_address varchar not null,
    start_dt timestamp default now(),
    end_dt timestamp,
    is_active bool default true,
    constraint pk_terminal_id_hist primary key (id)
)
distributed by (id);


create table dim_clients_hist
(
	id serial,
	client_id varchar not null,
	last_name varchar,
	first_name varchar not null,
	patronymic varchar,
	date_of_birth date not null,
	passport_num varchar not null,
	passport_valid_to date not null,
	phone varchar not null,
	start_dt timestamp default now(),
    end_dt timestamp,
    is_active bool default true,
	constraint pk_client_id_hist primary key (id) 
)
distributed by (id);

create table dim_accounts_hist
(
	id serial,
	account_num varchar not null,
	valid_to date not null,
	client_id varchar not null,
	start_dt timestamp default now(),
    end_dt timestamp,
    is_active bool default true,
	constraint pk_account_num_hist primary key (id)
)
distributed by (id);


create table dim_cards_hist
(
	id serial,
    card_num varchar not null,
    account_num varchar not null,
    start_dt timestamp default now(),
    end_dt timestamp,
    is_active bool default true,
    constraint pk_card_num primary key (id)
)
distributed by (id);


create table fact_transactions
( 
    trans_id int,
    trans_date timestamp not null,
    card_num varchar not null,
    oper_type varchar not null,
    amt decimal not null check(amt > 0),
    oper_result varchar not null,
    terminal_id varchar not null,
    constraint pk_trans_id primary key (trans_id)
)
distributed by (trans_id);


create table report
(
	fraud_id serial, 
    fraud_dt timestamp not null,
    passport varchar not null,
    fio varchar not null,
    phone varchar not null,
    fraud_type varchar not null,
    report_dt timestamp not null,
    constraint pk_fraud_id primary key (fraud_id)
)
distributed by (fraud_id);


create table stg_transactions
( 
    trans_id int,
    trans_date timestamp not null,
    card_num varchar not null,
    account_num varchar not null,
    valid_to date not null,
    client_id varchar not null,
    last_name varchar,
    first_name varchar not null,
    patronymic varchar,
    date_of_birth date not null,
    passport_num varchar not null,
    passport_valid_to date not null,
    phone varchar not null,
    oper_type varchar not null,
    amt decimal not null check(amt > 0),
    oper_result varchar not null,
    terminal_id varchar not null,
    terminal_type varchar not null,
    terminal_city varchar not null,
    terminal_address varchar not null,
    constraint pk_stg_trans_id primary key (trans_id)
)
distributed by (trans_id);

create table meta_accounts
(
	id serial,
	event varchar not null,
	start_dt timestamp default now(),
	constraint pk_meta_accounts_id primary key (id)
)
distributed by (id);

create table meta_cards
(
	id serial,
	event varchar not null,
	start_dt timestamp default now(),
	constraint pk_meta_cards_id primary key (id)
)
distributed by (id);

create table meta_clients
(
	id serial,
	event varchar not null,
	start_dt timestamp default now(),
	constraint pk_meta_clients_id primary key (id)
)
distributed by (id);

create table meta_terminals
(
	id serial,
	event varchar not null,
	start_dt timestamp default now(),
	constraint pk_meta_terminals_id primary key (id)
)
distributed by (id);

create table meta_transactions
(
	id serial,
	event varchar not null,
	start_dt timestamp default now(),
	constraint pk_meta_transactions_id primary key (id)
)
distributed by (id);

create table meta_report
(
	id serial,
	event varchar not null,
	start_dt timestamp default now(),
	constraint pk_meta_report_id primary key (id)
)
distributed by (id);