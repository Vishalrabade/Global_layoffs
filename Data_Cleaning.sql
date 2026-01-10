-- Data Cleaning

select *
from layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the data
-- 3. Deal with Null values and blank values
-- 4. Remove any columns

create table layoffs_staging
like layoffs;

select *
from layoffs_staging;

insert layoffs_staging
select *
from layoffs;

-- 1. Remove Duplicates

select *,
row_number() over(partition by company, location, industry, total_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;


With duplicate_cte as
(
select *,
row_number() over(partition by company, location, industry, total_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;

select * 
from layoffs_staging
where company = 'casper'; 

With duplicate_cte as
(
select *,
row_number() over(partition by company, location, industry, total_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
delete
from duplicate_cte
where row_num > 1;

CREATE TABLE `layoffs_staging2` (
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

select *
from layoffs_staging2;

insert into layoffs_staging2
select *,
row_number() over(partition by company, location, industry, total_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

delete
from layoffs_staging2
where row_num > 1;

select *
from layoffs_staging2
where row_num > 1;

-- 2. Standardizing data

select distinct(company), trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select distinct(industry)
from layoffs_staging2
order by 1;

select *
from layoffs_staging2
where industry like 'crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

update layoffs_staging2
set industry = trim(industry);

select distinct location
from layoffs_staging2
order by 1;

select * 
from layoffs_staging2
where location like '%sseldorf';

update layoffs_staging2
set location = 'Dusseldorf'
where location like '%sseldorf';

select * 
from layoffs_staging2
where location like 'flo%';

update layoffs_staging2
set location = 'Florianopolis'
where location like 'flo%';

select * 
from layoffs_staging2
where location like 'malm%';

update layoffs_staging2
set location = 'Malmo'
where location like 'malm%';

select *
from layoffs_staging2;

update layoffs_staging2
set location = trim(location);

select distinct country
from layoffs_staging2
order by 1;

select *
from layoffs_staging2
where country like 'United States.';

update layoffs_staging2
set country = 'United States'
where country like 'United States.';

-- another method to deal with country

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country =  trim(trailing '.' from country)
where country like 'United States%';

update layoffs_staging2
set country = trim(country);

select *
from layoffs_staging2;

select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

select *
from layoffs_staging2
where `date` is null; -- we will deal it later

alter table layoffs_staging2
modify column `date` date;

-- 3. Null and Blank Values

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select distinct industry
from layoffs_staging2;

update layoffs_staging2
set industry = null
where industry = '';

select *
from layoffs_staging2
where industry is null
or industry = ''; -- now we can populate this data

select * from layoffs_staging2
where company = 'Airbnb';

select t1.industry, t2.industry
from layoffs_staging2 as t1
join layoffs_staging2 as t2
	on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

update layoffs_staging2 as t1
join layoffs_staging2 as t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

select *
from layoffs_staging2
where industry is null
or industry = '';

select * from layoffs_staging2
where company like 'Bally%';

select * 
from layoffs_staging2;

select * 
from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;

-- there is no other null values that we can populates
--  now if there is null values that we can not populates and which are not impactfull or unuse then we have to get rid off it
-- before deleting always consider twice and from the angles
-- now we can delete total_laid_off and percentage_laid_off because we can not populate it and both are unuse for use

delete
from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;

-- 4. delete column which is not important or created by you

alter table layoffs_staging2
drop column row_num;

select * 
from layoffs_staging2;

