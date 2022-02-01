
do $$ 
declare
   client_target_count integer := 100000;
   action_target_count integer :=  1000000000;
   big_client_rate float := 0.2;
   
   action_period_days  integer := 90;
   action_actual_period_days_rate float := 0.2;
   
   client_in_step_count integer := 1;
   action_body text := fn_get_random_string(20000);
begin 
	
   --fill client
   insert into clients(client_id, name, registation_timestamp, action_count, delete_timestamp)
   select 
	 sq.client_id
	,sq.name
	,sq.registation_timestamp	
	--make random different action_count between client same level
	,sq.action_count
		+  ((case when random() <= 0.5 then -1 else 1 end) 
		*  floor(action_count * random()))
	 as action_count
	,sq.delete_timestamp
  from (
	select 
		 g as client_id
		,concat('client_', g) as name
		,now() 
			- interval '1 day' * action_period_days 
			- interval '1 day' * (random() * 100)
		 as registation_timestamp
	    --generate random client action_count for two kind  client (big and small) according to probability = big_client_rate
		,case 
			when random() <= big_client_rate
			then (action_target_count * (1 - big_client_rate))/(client_target_count * (big_client_rate))
			else (action_target_count * (big_client_rate))/(client_target_count * (1 - big_client_rate))
		 end as action_count
		,case 
	  		--every ~20 client deleted
			when random() <= 0.05 
			then now() - interval '1 day' * (random()/3 * 100)
			else NULL
		 end as delete_timestamp
	from generate_series(1, client_target_count) as g 
	) as sq;
	
	--fill action in loop 
	for i in 1..client_target_count by client_in_step_count loop  
	    
	    insert into action(client_id, create_timestamp, action_body)
	  	select 
			 sq.client_id
			,sq.create_timestamp
			,action_body
		from (
			select 
				 sq.client_id
				,sq.create_timestamp
			from (
				select 
					 sq.client_id
					,sq.delete_timestamp
					,sq.action_count
					,now() 
							- interval '1 day' * 
					--generate create_timestamp with probability = action_actual_period_days_rate in actual period
					floor(case 
						when random() <= action_actual_period_days_rate
						then random() * (action_period_days - floor(action_actual_period_days_rate * action_period_days)) + floor(action_actual_period_days_rate * action_period_days) 
						else random() * floor(action_actual_period_days_rate * action_period_days) 
					 end)
							- interval '1 day' * (random())
					as create_timestamp	
				from clients as sq
				--generate rows count fro client = action_count
				inner join lateral generate_series(1, sq.action_count) as g on true
				where sq.client_id between i and (i + client_in_step_count) - 1 
			) as sq		
			where coalesce(sq.delete_timestamp, now()) > sq.create_timestamp 
		) as sq
		order by sq.create_timestamp desc;
		
   end loop; 	
end $$;