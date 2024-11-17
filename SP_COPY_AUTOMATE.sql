-- if want to load common files from stag

-- Create databae and schema
Create or replace Database Auto_Copy;
create or replace schema Dlink ;

-- use database and schema
use Database Auto_Copy;
use schema Dlink ;

-- create Stage
CREATE STAGE dlink_stg
DIRECTORY = ( ENABLE = true );

-- loaded csv in stg
SHOW STAGES;
LIST @AUTO_COPY.DLINK.DLINK_STG/;


-- create file format
CREATE OR REPLACE FILE FORMAT my_csv_format
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  NULL_IF = ('NULL', 'null')
  COMPRESSION = 'NONE';


-- create tables
create or replace table employee
(E_ID Number,NAME Varchar,EMAIL Varchar,PHONE_NUMBER Number);
select * from employee;

create or replace table department
(O_ID Number,NAME Varchar, Type_of_work Varchar);
select * from department;

create or replace table customer
(C_ID Number,NAME Varchar,Height Varchar,Weight Varchar);
select * from customer;

show tables;

-- Create config table
create or replace table config_table
(ID integer autoincrement start 1 increment 1,
db_name varchar,
schema_name varchar,
STAGE_NAME varchar,
FILE_FMT varchar,
TRG_TBL_NAME varchar,
TRG_TBL_path varchar,
file_name varchar,
PROJECT Varchar,
enabled boolean
);


-- Inser data
 Insert into AUTO_COPY.DLINK.config_table(DB_NAME, SCHEMA_NAME, STAGE_NAME, FILE_FMT, TRG_TBL_NAME, TRG_TBL_path, FILE_NAME, PROJECT, enabled ) Values
('Auto_Copy' ,'Dlink' ,'DLINK_STG' ,'my_csv_format' ,'CUSTOMER' ,'Auto_Copy.Dlink.CUSTOMER' ,'customer' ,'Migration' ,'TRUE' ),
('Auto_Copy' ,'Dlink' ,'DLINK_STG' ,'my_csv_format' ,'EMPLOYEE' ,'Auto_Copy.Dlink.EMPLOYEE' ,'employees' ,'Migration' ,'TRUE' ),
('Auto_Copy' ,'Dlink' ,'DLINK_STG' ,'my_csv_format' ,'DEPARTMENT' ,'Auto_Copy.Dlink.DEPARTMENT' ,'department' ,'Migration' ,'TRUE' );

select * from config_table;



Select 'CUSTOMER' , count(*) from CUSTOMER
union
Select 'EMPLOYEE', count(*) from EMPLOYEE
union
Select 'DEPARTMENT',count(*) from DEPARTMENT;



-- Create Procedure

CREATE OR REPLACE PROCEDURE COPY_AUTOMATE (PROJECT VARCHAR(100))
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    AS
    $$
    var msg = '';
    var return_value = "";

    try {
        // SQL query to fetch details from config_table for the given project
        var sql_cmd = `SELECT TRG_TBL_NAME, stage_name, file_name, file_fmt, DB_NAME,SCHEMA_NAME
                       FROM config_table
                       WHERE PROJECT = '${PROJECT}'`;

        // Create and execute the SQL statement
        var sql_stmt = snowflake.createStatement({ sqlText: sql_cmd });
        var rs = sql_stmt.execute();

        // Loop through the result set and execute COPY commands for each configuration
        while (rs.next()) {
            var tname = rs.getColumnValue(1);  // Target table name: TRG_TBL_NAME
            var sname = rs.getColumnValue(2);  // Stage name: stage_name
            var fname = rs.getColumnValue(3);  // File name: file_name (assuming its part of the config)
            var fmtname = rs.getColumnValue(4);  // File format name: file_fmt
            var dbname = rs.getColumnValue(5);  // Database Name
            var schname = rs.getColumnValue(6); // Schema Name

            // when you call the SP it will return the out put msg
            // Debugging output: Log the details being used for each copy (optional)
            msg += `Preparing to execute COPY INTO for table ${tname} from files starting with ${fname} in stage ${sname}.\n`;

            // Dynamically constructing the COPY INTO statement
            // Using the table name prefix (e.g., customer*, employee*) for the file matching
            var copy_data = `COPY INTO ${tname}
                             FROM @${dbname}.${schname}.${sname}
                             FILE_FORMAT = (FORMAT_NAME = '${fmtname}')
                             PATTERN = '.*${fname.toLowerCase()}.*\\.csv'`;  // Match files starting with the fname (e.g., customer*)

            // Output the constructed COPY INTO SQL command for debugging (optional)
            msg += `Constructed COPY INTO statement: ${copy_data}\n`;

            // Execute the COPY INTO statement
            var cpy_stmt = snowflake.createStatement({ sqlText: copy_data });
            var cpy_res = cpy_stmt.execute();

            // Capture results (optional)
            msg += `COPY command executed for table ${tname}.\n`;
        }
    } catch (err) {
        msg += "\nMessage: " + err.message;
        msg += "\nStack Trace:\n" + err.stackTraceTxt;
        return msg;
    }

    // Return success message with detailed logs
    return "Success: Files loaded into respective tables.\n" + msg;
    $$;

-- call procedure

call COPY_AUTOMATE('Migration');

-- o/p
Success: Files loaded into respective tables.
Preparing to execute COPY INTO for table CUSTOMER from files starting with customer in stage DLINK_STG.
Constructed COPY INTO statement: COPY INTO CUSTOMER
                             FROM @Auto_Copy.Dlink.DLINK_STG
                             FILE_FORMAT = (FORMAT_NAME = 'my_csv_format')
                             PATTERN = '.*customer.*\.csv'
COPY command executed for table CUSTOMER.
Preparing to execute COPY INTO for table EMPLOYEE from files starting with employees in stage DLINK_STG.
Constructed COPY INTO statement: COPY INTO EMPLOYEE
                             FROM @Auto_Copy.Dlink.DLINK_STG
                             FILE_FORMAT = (FORMAT_NAME = 'my_csv_format')
                             PATTERN = '.*employees.*\.csv'
COPY command executed for table EMPLOYEE.
Preparing to execute COPY INTO for table DEPARTMENT from files starting with department in stage DLINK_STG.
Constructed COPY INTO statement: COPY INTO DEPARTMENT
                             FROM @Auto_Copy.Dlink.DLINK_STG
                             FILE_FORMAT = (FORMAT_NAME = 'my_csv_format')
                             PATTERN = '.*department.*\.csv'
COPY command executed for table DEPARTMENT.




Select 'CUSTOMER' , count(*) from CUSTOMER
union
Select 'EMPLOYEE', count(*) from EMPLOYEE
union
Select 'DEPARTMENT',count(*) from DEPARTMENT;
