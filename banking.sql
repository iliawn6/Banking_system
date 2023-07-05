CREATE TABLE account (
    username VARCHAR(30) UNIQUE NOT NULL,
    accountNumber BIGINT UNIQUE,
    pass VARCHAR(30) NOT NULL,
    first_name VARCHAR(30) NOT NULL,
    last_name VARCHAR(30) NOT NULL,
    national_id VARCHAR(10) NOT NULL ,
    date_of_birth DATE NOT NULL,
    typee VARCHAR(20) NOT NULL ,
    interest_rate INT NOT NULL,
    PRIMARY KEY(username)
);

--CHECK(LEN(national_id) == 10 AND national_id NOT LIKE '%[^0-9]%'),
    --CHECK(typee == 'client' OR typee == 'employee')    

CREATE TABLE login_log(
    username VARCHAR(30) NOT NULL,
    login_time TIMESTAMP,
    CONSTRAINT fk_login_log FOREIGN KEY(username) REFERENCES account(username)
);

CREATE TABLE transact(
    typee VARCHAR(30) NOT NULL,
    transaction_time TIMESTAMP NOT NULL,
    fromm BIGINT,
    too BIGINT,
    amount INT NOT NULL,
    CONSTRAINT fk_transaction FOREIGN KEY(fromm) REFERENCES account(accountNumber),
    CONSTRAINT fk_transaction FOREIGN KEY(too) REFERENCES account(accountNumber), 
    CHECK(typee = 'deposit' OR typee = 'withdraw' OR
     typee = 'transfer' OR typee = 'interest')
);

CREATE TABLE latest_balances(
    accountNumber BIGINT UNIQUE NOT NULL,
    amount INT NOT NULL,
    FOREIGN KEY(accountNumber) REFERENCES account(accountNumber)
);

CREATE TABLE snapshot_log(
    snapshot_id BIGSERIAL NOT NULL,
    snapshot_timestamp TIMESTAMP NOT NULL
);

/*CHECK(LEN(accountNumber) == 16 AND accountNumber NOT LIKE '%[^0-9]%'),*/

/*
CREATE FUNCTION random_generator()
    returns INT AS
    $$
    BEGIN
        RETURN (1000000000000000 + floor(random() * 999999999999999));
    END;
    $$ language 'plpgsql' STRICT;



CREATE TRIGGER set_account_number BEFORE INSERT ON account 
    FOR EACH ROW 
    SET account.accountNumber = random_generator();

*/



CREATE FUNCTION reg_trig_func() RETURNS TRIGGER
    LANGUAGE plpgsql
    AS 
    $$
    BEGIN
        IF NOT(NEW.typee = 'client' OR NEW.typee = 'employee') then
            RAISE EXCEPTION 'Type must be client or employee';
        END IF;

        IF NOT(length(NEW.national_id) = 10 AND NEW.national_id LIKE '%[^0-9]%') then
            RAISE EXCEPTION 'National_id must be an integer with length of 10';
        END IF;    

        IF (date_part('year', age(NEW.date_of_birth))::int) < 13 then
            RAISE EXCEPTION 'you are too young!!';
        END IF;  

        IF NEW.typee = 'employee' then
            NEW.interest_rate = 0;
        END IF;
        
        NEW.accountNumber = 1000000000000000 + floor(random() * 999999999999999);
        RETURN NEW;
    END;
    $$;
    --accountNumbers are not unique

CREATE TRIGGER reg_trig BEFORE INSERT ON account
    FOR EACH ROW EXECUTE PROCEDURE reg_trig_func();



INSERT INTO snapshot_log(snapshot_timestamp)
values(CURRENT_TIMESTAMP);

CREATE FUNCTION accNum(inp_username VARCHAR(30)) RETURNS BIGINT 
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN(SELECT accountNumber
    From account
    where inp_username = account.username);
END;
$$;    

CREATE PROCEDURE register (
    inp_username VARCHAR(30),
    inp_pass VARCHAR(30),
    inp_first_name VARCHAR(30),
    inp_last_name VARCHAR(30),
    inp_national_id VARCHAR(10),
    inp_date_of_birth DATE,
    inp_typee VARCHAR(20),
    inp_interest_rate integer
)
LANGUAGE plpgsql
AS $$
BEGIN
    --TODO password must be hashed!!!
    INSERT INTO account(username, pass, first_name,
    last_name, national_id, date_of_birth, typee, interest_rate)
    values(inp_username, inp_pass, inp_first_name, inp_last_name, 
    inp_national_id, inp_date_of_birth, inp_typee, inp_interest_rate);
    RAISE NOTICE 'register successfully done!!!';

    INSERT INTO latest_balances(accountNumber, amount)
    values(accNum(inp_username), 0);
END;
$$;


CREATE procedure login(inp_username VARCHAR(30), inp_pass VARCHAR(30))
language plpgsql 
AS $$
declare
    temp1 VARCHAR(30);
BEGIN
    SELECT pass into temp1
        from account 
        where account.username = inp_username;
    if temp1 = inp_pass then 
        insert into login_log(username, login_time)
        values (inp_username, CURRENT_TIMESTAMP);
        RAISE NOTICE 'Login successful!!!';
    ELSE
        RAISE NOTICE 'Your username or password is incorrect!!!';    
    end if;    
END;
$$;


CREATE procedure deposit(inp_amount integer)
language plpgsql 
AS 
$$
declare temp BIGINT;
BEGIN
    SELECT accountNumber into temp
        From login_log, account
        where login_log.username = account.username
        ORDER BY login_time DESC
        LIMIT 1;
    
    INSERT into transact(typee, transaction_time,fromm, too, amount)
    values ('deposit', CURRENT_TIMESTAMP, NULL, temp, inp_amount);   
END;
$$;

CREATE procedure withdraw(inp_amount integer)
language plpgsql
AS 
$$
declare temp BIGINT;
BEGIN
    SELECT accountNumber into temp
        From login_log, account
        where login_log.username = account.username
        ORDER BY login_time DESC
        LIMIT 1;  
    INSERT into transact(typee, transaction_time,fromm, too, amount)
    values ('withdraw', CURRENT_TIMESTAMP, temp, NULL, inp_amount);   
END;
$$;

CREATE procedure transferr(inp_amount integer, inp_accountNumber BIGINT)
language plpgsql 
AS 
$$
declare 
    temp VARCHAR(30);
    temp2 BIGINT;
BEGIN
       SELECT accountNumber into temp2
        From login_log, account
        where login_log.username = account.username
        ORDER BY login_time DESC
        LIMIT 1;
        
    SELECT username into temp
    from account
    where inp_accountNumber = accountNumber;
    IF temp != NULL then
        INSERT into transact(typee, transaction_time,fromm, too, amount)
        values ('transfer', CURRENT_TIMESTAMP, temp2,
         inp_accountNumber, inp_amount);   
    ELSE
        RAISE NOTICE 'Destination account does not exists!!!';  
    END IF;       
END;
$$;

CREATE procedure interest_payment()
language plpgsql 
AS 
$$
declare 
    temp INT;
    temp1 INT;
    accNumber BIGINT;
    amountBalance INT;
BEGIN
    SELECT account.interest_rate,account.accountNumber into temp1,accNumber
    from login_log, account
    where login_log.username = account.username
    ORDER BY login_time DESC
    LIMIT 1; 

    SELECT amount into amountBalance
    From  latest_balances, account
    where account.accountNumber = accNumber;

    INSERT into transact(typee, transaction_time,fromm, too, amount)
    values ('interest', CURRENT_TIMESTAMP, NULL, accNumber, amountBalance * temp1);   

END;
$$;


CREATE procedure update_balances()
language plpgsql
AS 
$$
declare
    temp_time TIMESTAMP;
    rec record;
    temp integer;
    check_type VARCHAR(30);
    table_id integer;
BEGIN
    SELECT typee into check_type
    FROM account, (SELECT * FROM login_log ORDER BY login_time DESC LIMIT 1) as lt
    where account.username = lt.username;

    IF check_type = 'employee' then

        SELECT snapshot_timestamp into temp_time
        FROM snapshot_log
        ORDER BY snapshot_id DESC
        LIMIT 1;

        for rec in (SELECT * from transact where transact.transaction_time > temp_time)
        loop

            IF rec.typee = 'deposit' then
                UPDATE latest_balances SET amount = amount + rec.amount
                where accountNumber = rec.too;
            END IF;

            IF rec.typee = 'whithdraw' then
                SELECT amount into temp
                from latest_balances
                where rec.fromm = latest_balances.accountNumber;

                IF temp >= rec.amount then
                    UPDATE latest_balances SET amount = amount - rec.amount
                    where accountNumber = rec.fromm;
                END IF;
            END IF;

            IF rec.typee = 'transfer' then
                SELECT amount into temp
                from latest_balances
                where rec.fromm = latest_balances.accountNumber;
               
                IF temp >= rec.amount then
                    UPDATE latest_balances SET amount = amount - rec.amount
                    where accountNumber = rec.fromm;

                    UPDATE latest_balances SET amount = amount + rec.amount
                    where accountNumber = rec.too;
                END IF;
            END IF;

            IF rec.typee = 'interest' then
                UPDATE latest_balances SET amount = amount + rec.amount
                where accountNumber = rec.too;
            END IF;

        end loop;


        INSERT INTO snapshot_log(snapshot_timestamp)
        values(CURRENT_TIMESTAMP);

        SELECT snapshot_id into table_id
        FROM snapshot_log
        ORDER BY snapshot_id DESC
        LIMIT 1;


        EXECUTE format('CREATE TABLE snapshot AS TABLE latest_balances;');
    ELSE
        RAISE NOTICE 'you do not have access!!!';      
    END IF;
END;
$$;


   

create procedure check_balance()
language plpgsql
AS
$$
declare
    temp BIGINT;
    res int;
BEGIN
    SELECT account.accountNumber into temp
    from login_log, account
    where login_log.username = account.username
    ORDER BY login_time DESC
    LIMIT 1; 

    SELECT amount into res
    FROM latest_balances
    where latest_balances.accountNumber = temp;

    RAISE NOTICE '%', res;
    
END;
$$;




/*print output for every procedure*/



