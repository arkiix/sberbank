create or replace function load_terminals() 
returns void as
$body$
begin
	insert into frod_logs (log_stage, log_event)
	values ('Выгрузка данных из stg_transactions', 'Загрузка данных в dim_terminals_hist');
	
	insert into dim_terminals_hist (terminal_id, terminal_type, terminal_city, terminal_address)
    select distinct t1.terminal_id, t1.terminal_type, t1.terminal_city, t1.terminal_address 
    from stg_transactions t1
	left join dim_terminals_hist t2 on t1.terminal_id = t2.terminal_id 
    where coalesce(t2.is_active, true) = true
    	and (t2.is_active is null
    	or (t1.terminal_type, t1.terminal_city, t1.terminal_address) != (t2.terminal_type, t2.terminal_city, t2.terminal_address));
	
    insert into frod_logs (log_stage, log_event)
	values ('Выгрузка данных из stg_transactions', 'Обновление историчности dim_terminals_hist');
	
    update dim_terminals_hist
    set end_dt = now(), is_active = false
    where id in (
	    select distinct t1.id
	    from dim_terminals_hist t1
	    join dim_terminals_hist t2 on t1.terminal_id = t2.terminal_id 
	    where t1.id < t2.id
	    	and t1.is_active = true
    );
   
   	insert into frod_logs (log_stage, log_event)
	values ('Выгрузка данных из stg_transactions', 'Обновление meta_terminals');

   	insert into meta_terminals (rec_cnt)
	select count(*)
	from dim_terminals_hist;
exception
  	when others then
	   	insert into frod_logs (log_stage, log_event)
		values ('Выгрузка данных из stg_transactions', 'Ошибка: ' || sqlstate);
end; 
$body$
	language plpgsql;


create or replace function load_clients() 
returns void as
$body$
begin
	insert into frod_logs (log_stage, log_event)
	values ('Выгрузка данных из stg_transactions', 'Загрузка данных в dim_clients_hist');
	
	insert into dim_clients_hist (client_id, last_name, first_name, patronymic, 
		date_of_birth, passport_num, passport_valid_to, phone)
    select distinct t1.client_id, t1.last_name, t1.first_name, t1.patronymic, 
		t1.date_of_birth, t1.passport_num, t1.passport_valid_to, t1.phone 
	from stg_transactions t1
	left join dim_clients_hist t2 on t1.client_id = t2.client_id
    where coalesce(t2.is_active, true) = true
    	and (t2.is_active is null
    	or (t1.last_name, t1.first_name, t1.patronymic, t1.date_of_birth, t1.passport_num, t1.passport_valid_to, t1.phone) != (t2.last_name, t2.first_name, t2.patronymic, t2.date_of_birth, t2.passport_num, t2.passport_valid_to, t2.phone)); 
	
    insert into frod_logs (log_stage, log_event)
	values ('Выгрузка данных из stg_transactions', 'Обновление историчности dim_clients_hist');
	
    update dim_clients_hist
    set end_dt = now(), is_active = false
    where id in (
	    select distinct t1.id
	    from dim_clients_hist t1
	    join dim_clients_hist t2 on t1.client_id = t2.client_id	
	    where t1.id < t2.id
	    	and t1.is_active = true
	);
   
	insert into frod_logs (log_stage, log_event)
	values ('Выгрузка данных из stg_transactions', 'Обновление meta_clients');

   	insert into meta_clients (rec_cnt)
	select count(*)
	from dim_clients_hist;
exception
  	when others then
	   	insert into frod_logs (log_stage, log_event)
		values ('Выгрузка данных из stg_transactions', 'Ошибка: ' || sqlstate);
end; 
$body$
	language plpgsql;


create or replace function load_cards() 
returns void as
$body$
begin
	insert into frod_logs (log_stage, log_event)
	values ('Выгрузка данных из stg_transactions', 'Загрузка данных в dim_cards_hist');
	
	insert into dim_cards_hist (card_num, account_num)
    select distinct t1.card_num, t1.account_num
    from stg_transactions t1
	left join dim_cards_hist t2 on t1.card_num = t2.card_num
    where coalesce(t2.is_active, true) = true
    	and (t2.is_active is null
    	or t1.account_num != t2.account_num); 
	
    insert into frod_logs (log_stage, log_event)
	values ('Выгрузка данных из stg_transactions', 'Обновление историчности dim_cards_hist');
	
   	update dim_cards_hist
    set end_dt = now(), is_active = false
    where id in (
    	select distinct t1.id
    	from dim_cards_hist t1
    	join dim_cards_hist t2 on t1.card_num = t2.card_num
    	where t1.id < t2.id
    		and t1.is_active = true
    );

   	insert into frod_logs (log_stage, log_event)
	values ('Выгрузка данных из stg_transactions', 'Обновление meta_cards');

   	insert into meta_cards (rec_cnt)
	select count(*)
	from dim_cards_hist;
exception
  	when others then
	   	insert into frod_logs (log_stage, log_event)
		values ('Выгрузка данных из stg_transactions', 'Ошибка: ' || sqlstate);
end; 
$body$
	language plpgsql;


create or replace function load_accounts() 
returns void as
$body$
begin
	insert into frod_logs (log_stage, log_event)
	values ('Выгрузка данных из stg_transactions', 'Загрузка данных в dim_accounts_hist');
	
	insert into dim_accounts_hist (account_num, valid_to, client_id)
    select distinct t1.account_num, t1.valid_to, t1.client_id
    from stg_transactions t1
	left join dim_accounts_hist t2 on t1.account_num = t2.account_num 
    where coalesce(t2.is_active, true) = true
    	and (t2.is_active is null
    	or (t1.valid_to, t1.client_id) != (t2.valid_to, t2.client_id));
   
    insert into frod_logs (log_stage, log_event)
	values ('Выгрузка данных из stg_transactions', 'Обновление историчности dim_accounts_hist');
	
    update dim_accounts_hist
    set end_dt = now(), is_active = false
    where id in (
    	select distinct t1.id
    	from dim_accounts_hist t1
    	join dim_accounts_hist t2 on t1.account_num = t2.account_num 
    	where t1.id < t2.id
    		and t1.is_active = true
    );
   
   	insert into frod_logs (log_stage, log_event)
	values ('Выгрузка данных из stg_transactions', 'Обновление meta_accounts');
   
   	insert into meta_accounts (rec_cnt)
	select count(*)
	from dim_accounts_hist;
exception
  	when others then
	   	insert into frod_logs (log_stage, log_event)
		values ('Выгрузка данных из stg_transactions', 'Ошибка: ' || sqlstate);
end; 
$body$
	language plpgsql;


create or replace function load_transactions() 
returns void as
$body$
declare
	v_meta_cnt integer := 0;
	v_last_cnt integer := 0;
	v_cur_cnt integer := 0;
	v_avg decimal := 0;
begin
	insert into frod_logs (log_stage, log_event)
	values ('Выгрузка данных из stg_transactions', 'Загрузка данных в fact_transactions');
	
	insert into fact_transactions (trans_id, trans_date, card_num, oper_type,
    	amt, oper_result, terminal_id)
    select trans_id, trans_date, card_num, oper_type,
    	amt, oper_result, terminal_id
    from stg_transactions;

   	insert into frod_logs (log_stage, log_event)
	values ('Выгрузка данных из stg_transactions', 'Обновление meta_transactions');

   	select count(*)
   	into v_meta_cnt
	from meta_transactions;

	if v_meta_cnt != 0 then
	   	select rec_avg
	   	into v_avg
		from meta_transactions
		order by id desc
		limit 1;
	
	   	select rec_cnt
		into v_last_cnt
	   	from meta_transactions
		order by id desc
		limit 1;
		
		insert into meta_transactions (rec_cnt, rec_avg)
		select count(*), count(*) / (v_meta_cnt + 1)
   		from fact_transactions;
   	
	   	select rec_cnt
		into v_cur_cnt
	   	from meta_transactions
		order by id desc
		limit 1;
	
		if v_cur_cnt - v_last_cnt < v_avg then
			insert into frod_logs (log_stage, log_event)
			values ('Выгрузка данных из stg_transactions', 'Количество загруженных записей меньше среднего! Среднее кол-во: ' || v_avg || ' загружено: ' || v_cur_cnt - v_last_cnt);
		end if;
   	else
	   	insert into meta_transactions (rec_cnt, rec_avg)
		select count(*), count(*)
		from fact_transactions;
	end if;
exception
  	when others then
	   	insert into frod_logs (log_stage, log_event)
		values ('Выгрузка данных из stg_transactions', 'Ошибка: ' || sqlstate);
end; 
$body$
	language plpgsql;


create or replace function distribution_of_records() 
returns void as
$body$
begin
	insert into frod_logs (log_stage)
	values ('Выгрузка данных из stg_transactions');
	
	perform "load_terminals"();
    perform "load_clients"();
   	perform "load_cards"();
   	perform "load_accounts"();
   	perform "load_transactions"();
   	
    insert into frod_logs (log_stage, log_event)
	values ('Выгрузка данных из stg_transactions', 'Очистка stg_transactions');
   	truncate table stg_transactions;
exception
  	when others then
	   	insert into frod_logs (log_stage, log_event)
		values ('Выгрузка данных из stg_transactions', 'Ошибка: ' || sqlstate);
end; 
$body$
	language plpgsql;


create or replace function build_report() 
returns void as
$body$
begin
	insert into frod_logs (log_stage)
	values ('Построение отчёта');

	insert into frod_logs (log_stage, log_event)
	values ('Построение отчёта', 'Загрузка данных в stg_transactions');
	insert into stg_transactions 
    select t.trans_id, t.trans_date, c.card_num, ac.account_num, ac.valid_to, cl.client_id, cl.last_name,
    	cl.first_name, cl.patronymic, cl.date_of_birth, cl.passport_num, cl.passport_valid_to, cl.phone,
    	t.oper_type, t.amt, t.oper_result, te.terminal_id, te.terminal_type, te.terminal_city, te.terminal_address 
    from fact_transactions t
    join dim_terminals_hist te on t.terminal_id = te.terminal_id 
	join dim_cards_hist c on t.card_num = c.card_num
	join dim_accounts_hist ac on c.account_num = ac.account_num
	join dim_clients_hist cl on ac.client_id = cl.client_id
	where t.trans_date > (
			select coalesce(max(fraud_dt), to_timestamp(0))
			from report
		) - interval '1 hour'
		and cl.is_active = true 
		and ac.is_active = true 
		and c.is_active = true 
		and te.is_active = true;
    

	insert into frod_logs (log_stage, log_event)
	values ('Построение отчёта', 'Вставка записей (совершение операции при просроченном паспорте)');
	insert into report (fraud_dt, passport, fio, phone, fraud_type, report_dt)
    select max(trans_date), passport_num, last_name || ' ' || first_name || ' ' || patronymic, 
    	phone, 'совершение операции при просроченном паспорте', now()
	from stg_transactions t
	where trans_date > passport_valid_to
		and date_trunc('day', trans_date) = (
			select date_trunc('day', max(trans_date)) 
			from stg_transactions 
		)
	group by date_trunc('day', trans_date), client_id, last_name, first_name, patronymic, passport_num, phone; 
   

	insert into frod_logs (log_stage, log_event)
	values ('Построение отчёта', 'Вставка записей (совершение операции при недействующем договоре)');
    insert into report (fraud_dt, passport, fio, phone, fraud_type, report_dt)
    select max(trans_date), passport_num, last_name || ' ' || first_name || ' ' || patronymic, 
		phone, 'совершение операции при недействующем договоре', now() 
	from stg_transactions
	where trans_date > valid_to
		and date_trunc('day', trans_date) = (
			select date_trunc('day', max(trans_date)) 
			from stg_transactions 
		)
	group by date_trunc('day', trans_date), client_id, last_name, first_name, patronymic, passport_num, phone; 
   

	insert into frod_logs (log_stage, log_event)
	values ('Построение отчёта', 'Вставка записей (совершение операции в разных городах в течение 1 часа)');
    insert into report (fraud_dt, passport, fio, phone, fraud_type, report_dt)
    select max(t2.trans_date), t1.passport_num, t1.last_name || ' ' || t1.first_name || ' ' || t1.patronymic, 
		t1.phone, 'совершение операции в разных городах в течение 1 часа', now()
    from stg_transactions t1
    join stg_transactions t2 on t1.client_id = t2.client_id
    where 1 = 1
    	and t1.trans_id != t2.trans_id
    	and t1.trans_date < t2.trans_date
    	and t1.terminal_city != t2.terminal_city
    	and extract(epoch from t2.trans_date - t1.trans_date) / 3600 <= 1
    	and (date_trunc('day', t1.trans_date) < date_trunc('day', t2.trans_date) 
    	or date_trunc('day', t1.trans_date) = (
	    		select date_trunc('day', max(trans_date))
	    		from stg_transactions 
    		)
    	)
    group by date_trunc('day', t2.trans_date), t1.client_id, t1.last_name, 
   		t1.first_name, t1.patronymic, t1.passport_num, t1.phone;
   
   	
   	insert into frod_logs (log_stage, log_event)
	values ('Построение отчёта', 'Обновление meta_report');
    
   	insert into meta_report (rec_cnt)
	select count(*)
	from report;


   	insert into frod_logs (log_stage, log_event)
	values ('Построение отчёта', 'Очистка stg_transactions');
   	truncate table stg_transactions;
exception
  	when others then
	   	insert into frod_logs (log_stage, log_event)
		values ('Построение отчёта', 'Ошибка: ' || sqlstate);
end;
$body$
	language plpgsql;