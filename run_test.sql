do $$ 
declare
   action_period_days  integer := 90;
   action_actual_period_days_rate float := 0.2;
   
   client_id integer;
   start_ts timestamp;
   end_ts timestamp;
	
   start_query timestamp;
   end_query  timestamp;
   ms_time_query integer;
   row_count_query integer;
   byte_size_query bigint;
   
   minute_test_count integer := 5;
   start_test_ts timestamp;
   stop_test_ts timestamp;
   minute_from_start_test integer:= 0;
   description_test text := 'запуск 10';
   
   total_size_query text;   
begin 	
	
	start_test_ts := now();
	stop_test_ts := start_test_ts + (minute_test_count * interval '1 minute');
	minute_from_start_test := 0;

	while (clock_timestamp() <= stop_test_ts) loop		
		
		client_id := fn_get_random_client(action_actual_period_days_rate);
		start_ts := fn_get_random_date(action_period_days, action_actual_period_days_rate);
		end_ts := start_ts + interval '1 day' * random() * floor(extract(epoch from now() - start_ts)/86400); 

		start_query := clock_timestamp();	
		 
		drop table if exists temptable;
		
		create table temptable as 
		select *
		from fn_get_action(client_id, start_ts, end_ts);	
		
		end_query := clock_timestamp();
		
		ms_time_query := 1000 * (extract(epoch from end_query - start_query));
		get diagnostics row_count_query = row_count;
		
		byte_size_query :=pg_total_relation_size('temptable');
		
		--save every query test result 
		insert into testlog_minute (client_id, ts_from, ts_to, 
								    row_count, byte_size_query, ms_time_query)
		select client_id, start_ts, end_ts, 
			   row_count_query, byte_size_query, ms_time_query;
				 
		if minute_from_start_test <> DATE_PART('minute', clock_timestamp() - start_test_ts)
		then 
			minute_from_start_test := DATE_PART('minute', clock_timestamp() - start_test_ts);
				
			--aggregate every query test result to result per minute
		    insert into testlog (longest_request_client_id, longest_request_ts_from, longest_request_ts_to, 
								 row_count_per_minute, byte_size_query_per_minute, avg_time_query_per_minute, 
								 query_count_per_minute, minute_num, start_ts, description)	   	  
			select longest_request.client_id, longest_request.ts_from, longest_request.ts_to
					,sum_request.row_count, sum_request.byte_size_query, sum_request.ms_time_query
					,sum_request.query_count, minute_from_start_test, start_test_ts, description_test
			from (
				select t.client_id, t.ts_from, t.ts_to
				from testlog_minute as t
				order by t.ms_time_query desc
				limit 1
			) as longest_request
				cross join 
			(
				select sum(row_count) as row_count, sum(t.byte_size_query) as byte_size_query, avg(t.ms_time_query) as ms_time_query, count(*) as query_count
				from testlog_minute as t
			) sum_request;
			
			total_size_query := pg_size_pretty((select sum(byte_size_query_per_minute) from testlog as tt where tt.start_ts = start_test_ts));
			
			raise notice 'test info per minute %', 
						  (	
								select concat('iteration number:', minute_num, ' rate rows:', row_count_per_minute, 
								  ' rate kb:', byte_size_query_per_minute/1024, ' longest_request_client_id:', longest_request_client_id,
								  '  total data retrieved ', total_size_query)
								from testlog as t
								where t.start_ts = start_test_ts
									and t.minute_num = minute_from_start_test
							  	limit 1
					  	  );
			
		    truncate table testlog_minute;
	
		end if;

	end loop;		
end $$;