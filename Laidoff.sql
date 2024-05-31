-- 1. Remove duplicates
-- 2. Standardize the data
-- 3. Blank values or Null values
-- 4. Remove Any columns


select * from layoffs;

-- To preserve Original Data, a new table 'Staging' is created here all the data cleaning work is done.

create table staging like layoffs;

-- All the data from layoffs table is inserted into staging table

insert staging
select * from layoffs;

select *,
row_number () over
(partition by company, location, industry, total_laid_off, percentage_laid_off,
 `date`, stage, country, funds_raised_millions) as rn
 from staging;

with cte as (
	select *,
	row_number () over
	(partition by company, location, industry, total_laid_off, percentage_laid_off,
	 `date`, stage, country, funds_raised_millions) as rn
	 from staging
)
select * from cte
where rn > 1;


-- A new table staging2 is created with an additional column row_num

CREATE TABLE `staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert staging2
select *,
row_number () over
(partition by company, location, industry, total_laid_off, percentage_laid_off,
 `date`, stage, country, funds_raised_millions) as rn
 from staging;

-- Removing Duplicates (Delete the rows where row_num > 1)

select * from staging2
where row_num > 1;

delete from staging2
where row_num > 1;

select * from staging2;


-- standardizing data

select * from staging2;

-- Unnecessary spaces in the column 'company' are removed

update staging2
set company = trim(company);

select distinct(industry) from staging2
order by 1;

-- Companies having the industry name as variations of crypto are grouped in one single name 'Crypto'

select * from staging2
where industry like '%crypto%';

update staging2
set industry = 'Crypto'
where industry like '%crypto%';

-- Some country name have a trailing '.' after them creating multiple variations of the same country name. This can be standerdized by removing the '.' at the end.

select distinct(country), trim(trailing '.' from country) from staging2
order by 1;

update staging2
set country = trim(trailing '.' from country);


-- `date` column is fixed and the datatype of the `date` column is changed to 'date'.

select `date` from staging2;

update staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

alter table staging2
modify column `date` date;

-- Checking for blank or null values in industry cloumn

select * from staging2
where industry = '';

-- Companies like airbnb have a few rows where industry column is blank we can populate those columns by using the values from different rows of the same company.

select * from staging2
where company like 'airbnb%';

select * from staging2 t1
		 join staging2 t2
	on t1.company = t2.company
	  and t1.location = t2.location
      where t1.industry = '';

select t1.industry, t2.industry 
		 from staging2 t1
		 join staging2 t2
	on t1.company = t2.company
	  and t1.location = t2.location
      where t1.industry = '';

update staging2 t1
	   join staging2 t2
	   on t1.company = t2.company
	and t1.location = t2.location
set t1.industry = t2.industry
	  where t1.industry = ''
      and t2.industry <> '';

-- The null values in total_laid_off, percentage_laid_off, and funds_raised_millions looks good
-- Unnecessary rows and columns which cannot be used during EDA phase are removed.

select * from staging2
where total_laid_off is null and percentage_laid_off is null;

delete from staging2
where total_laid_off is null and percentage_laid_off is null;

alter table staging2
drop column row_num;

-- -----------------------------------------------------------------Exploratory Data Analysis ---------------------------------------------------------------------------

-- Max KPI

select * from staging2;
select max(total_laid_off), max(percentage_laid_off),
	   max(funds_raised_millions) from staging2;

-- Companies with Complete layoffs by funding

select * from staging2
where percentage_laid_off = '1'
order by funds_raised_millions desc;

-- Total funding raised by the Companies where Complete layoffs happened.

select sum(funds_raised_millions) from staging2
where percentage_laid_off = '1';

-- Total number layoffs by each company

select company, sum(total_laid_off)
from staging2
group by company
order by 2 desc;

-- Date range and total layoffs

select min(`date`), max(`date`), sum(total_laid_off) from staging2;

-- Total number of layoffs by Industry

select industry, sum(total_laid_off) from staging2
group by industry
order by 2 desc;

-- Total number of layoffs by Country

select country, sum(total_laid_off) from staging2
group by country
order by 2 desc;

-- Total number of layoffs by Date

-- Day

select `date`, sum(total_laid_off) from staging2
group by `date`
order by sum(total_laid_off) desc;

-- Year

select year(`date`), sum(total_laid_off) from staging2
group by year(`date`)
order by sum(total_laid_off) desc;

-- Month

select month(`date`), sum(total_laid_off) from staging2
group by month(`date`)
order by sum(total_laid_off) desc;

-- Year, Month

select year(`date`), month(`date`), sum(total_laid_off),
	rank () over (partition by year(`date`) order by month(`date`)) as rn
from staging2
group by month(`date`), year(`date`);

-- Total number of layoffs by Stage of the Company

select stage, sum(total_laid_off) from staging2
group by stage
order by sum(total_laid_off) desc;


-- Cumulative layoffs by Day

select `date`, total_laid_off,
		sum(total_laid_off) over(order by `date`) as rn
from staging2;

-- Cumulative layoffs by Year

with cte as(
select year(`date`) as yr,sum(total_laid_off) as off
from staging2
group by year(`date`)
order by year(`date`)
)
select yr, off, sum(off) over (order by yr) as cumulative
from cte;

-- Cumulative layoffs by Month-Year

select substring(`date`,1,7) as y_m, sum(total_laid_off) as off,
		sum(sum(total_laid_off)) over (order by substring(`date`,1,7)) as cumulative
from staging2
group by substring(`date`,1,7)
order by substring(`date`,1,7);


-- Top 5 Company layoffs per Year

select year(`date`), company, sum(total_laid_off),
		dense_rank () over (partition by year(`date`) order by sum(total_laid_off) desc) as rn
from staging2
group by year(`date`), company;

with cte as (
	select year(`date`), company, sum(total_laid_off),
			dense_rank () over (partition by year(`date`) order by sum(total_laid_off) desc) as rn
	from staging2
	group by year(`date`), company
)
select * from cte
where rn <= 5;

-- Cumulative layoffs by Companies/Year

select company, year(`date`), total_laid_off,
	dense_rank () over (partition by company order by year(`date`)) as rn
from staging2
where company = 'swiggy';

with cte as(
select company, year(`date`), total_laid_off,
	dense_rank () over (partition by company order by year(`date`)) as rn
from staging2
)
select * from cte where rn > 2;

with cte as (
select company as company, year(`date`) as yr, total_laid_off as layoffs,
	dense_rank () over (partition by company order by year(`date`)) as rn
from staging2
where company = 'swiggy')
select *, sum(layoffs) over (order by yr) as cumulative from cte;


with cte as (
select company, year(`date`) as yr, total_laid_off as `off`,
	dense_rank () over (partition by company order by year(`date`)) as rn
from staging2
where company = 'swiggy'
)
select company, yr, sum(`off`) from cte
group by company, yr;

with cte1 as (
				with cte as (
		select company, year(`date`) as yr, total_laid_off as `off`,
			dense_rank () over (partition by company order by year(`date`)) as rn
		from staging2
		where company = 'swiggy'
		)
		select company, yr, sum(`off`) as laid_of from cte
		group by company, yr
)
select *, sum(laid_of) over (order by yr) as cumulative from cte1;
