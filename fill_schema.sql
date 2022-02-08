
do $$ 
declare
   client_target_count integer := 100000;
   action_target_count integer :=  1000000000;
   
   action_period_days  integer := 90;
   
   action_body text := fn_get_random_string(20000);
begin 
   
   --fill client
   insert into clients(client_id, name, registation_timestamp, action_count, delete_timestamp)
   select 
	 sq.client_id
	,sq.name
	,sq.registation_timestamp	
	,sq.action_count
	,sq.delete_timestamp
  from (
	select 
		 g as client_id
		,concat('client_', g) as name
		,now() 
			- interval '1 day' * action_period_days 
			- interval '1 day' * (random() * 100)
		 as registation_timestamp
		,fn_get_distribution(client_target_count, action_target_count, g) as action_count
		,case 
	  		--every ~20 client deleted
			when random() <= 0.05 
			then now() - interval '1 day' * (random()/3 * 100)
			else NULL
		 end as delete_timestamp
	from generate_series(1, client_target_count) as g 
	) as sq;
	
	--fill action in loop 
	
	for i in 1..client_target_count by 1 loop  
	    
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
					,fn_get_random_date(action_period_days) as create_timestamp	
				from clients as sq
				--generate rows count for client = action_count
				inner join lateral generate_series(1, sq.action_count) as g on true
				where sq.client_id = i
			) as sq		
			where coalesce(sq.delete_timestamp, now()) > sq.create_timestamp 
		) as sq
		order by sq.create_timestamp desc;
		
   end loop; 	
   
end $$;
