if DB_ID('UrbanLife') is null
create database UrbanLife;
go
use UrbanLife;
go

if object_id('dbo.dim_city','U') is null
begin
create table dbo.dim_city (
   city_id int identity(1,1) primary key,
   city nvarchar(100) not null,
   country nvarchar(10) not null,
   latitude float not null,
   longitude float not null,
   constraint UQ_dim_city unique (city,country)
   );
   End
   Go

   if object_id('dbo.fact_city_daily', 'U') is null
   begin
   create table dbo.fact_city_daily (
   [date] date not null,
   city_id int not null,
   avg_temp float null,
   max_temp float null,
   min_temp float null,
   pm25_avg float null,
   extreme_flag bit null,
   life_score float null,
   updated_at datetime2 not null default sysutcdatetime(),

   Constraint PK_fact_city_daily primary key([date], city_id),
   Constraint FK_fact_city_daily_city foreign key (city_id) references dbo.dim_city(city_id)
   );
   End
   Go