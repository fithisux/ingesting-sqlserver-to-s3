use ncintegration;

IF NOT EXISTS ( SELECT  *
                FROM    sys.schemas
                WHERE   name = N'ncproject' )
    EXEC('CREATE SCHEMA ncproject');

IF SUSER_ID('nclogin') IS NULL
	create login nclogin with password='ncuser123!!';

IF DATABASE_PRINCIPAL_ID('ncuser') IS NULL
	create user ncuser for login nclogin with DEFAULT_SCHEMA = ncproject;

IF OBJECT_ID(N'ncproject.fintransacts', N'U') IS NULL
BEGIN
	create table ncproject.fintransacts(
	 id Int not Null,
	 last_transaction_date datetime,
	 description Varchar(200),
	 CONSTRAINT PK_FINTRANSACTS PRIMARY KEY (id)
	)
END;

IF OBJECT_ID(N'ncproject.ingestions', N'U') IS NULL
BEGIN
	create table ncproject.ingestions(
	 id Int not Null,
	 last_transaction_date datetime,
	 ingestion_date Varchar(200) not Null,
	 CONSTRAINT PK_INGESTIONS PRIMARY KEY (id)
	)
END;

Grant select on ncproject.fintransacts to ncuser;
Grant insert on ncproject.fintransacts to ncuser;
Grant update on ncproject.fintransacts to ncuser;
Grant delete on ncproject.fintransacts to ncuser;
Grant select on ncproject.ingestions to ncuser;
Grant insert on ncproject.ingestions to ncuser;
Grant update on ncproject.ingestions to ncuser;
Grant delete on ncproject.ingestions to ncuser;