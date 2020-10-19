--echo #
--echo # insert
--echo #
insert into foo values (1, 10);
call update_foo_history();
call print_idx_image(@image_number);

insert into foo values (2, 20);
call update_foo_history();
call print_idx_image(@image_number);

--echo #
--echo # update secondary index
--echo #
update foo set sec = 200 where id = 2;
call update_foo_history();
call print_idx_image(@image_number);

--echo #
--echo # delete
--echo #
delete from foo where id = 2;
call update_foo_history();
call print_idx_image(@image_number);

--echo #
--echo # insert on delete marked
--echo #
insert into foo values (2, 2000);
call update_foo_history();
call print_idx_image(@image_number);

--echo #
--echo # update pk
--echo #
update foo set id = 3 where id = 2;
call update_foo_history();
call print_idx_image(@image_number);

--echo #
--echo # force using secondary index
--echo #
insert into foo values (5, 50);
call update_foo_history();
set @time_stamp = (select image_time from foo_history where id = @image_number);
select @image_number, foo.* from foo as of timestamp @time_stamp force index (sec);


--echo #
--echo # can't see un-committed trx
--echo #
connection con2;
begin;
update foo set id = 6 where id = 5;
insert into foo values (7, 70);
update foo set id = 8 where id = 1;
delete from foo where id = 2;

connection con1;
call update_foo_history();
call print_idx_image(@image_number);

connection con2;
commit;

connection con1;
call update_foo_history();
call print_idx_image(@image_number);

--echo #
--echo # clear table
--echo #
delete from foo;
call update_foo_history();
