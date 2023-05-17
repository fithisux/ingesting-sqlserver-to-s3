use ncintegration;
drop table IF EXISTS ncproject.fintransacts;
drop table IF EXISTS ncproject.ingestions;
drop user IF EXISTS ncuser;

IF SUSER_ID('nclogin') IS NOT NULL
	drop login nclogin;

drop schema IF EXISTS ncproject;

use master;
drop database IF EXISTS ncintegration;