-- Data Cleaning 

select *
from layoffs;

-- 1 Remove Duplicates
-- 2 Standardize the data
-- 3 Null Values or Blank values
-- 4 Remove Any columns

create table layoffs_staging 
like layoffs;

select *
from layoffs_staging;

insert layoffs_staging
select *
from layoffs;

select *,
ROW_NUMBER() over(
Partition by company, industry, total_laid_off, percentage_laid_off, `date`) as row_num
from layoffs_staging;

WITH duplicate_cte  as
(select *,
ROW_NUMBER() over(
Partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select*
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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select*
from layoffs_staging2
where row_num > 1;

insert into layoffs_staging2
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;


DELETE	
from layoffs_staging2
where row_num > 1;

select*
from layoffs_staging2;


-- standardizing data
select company, trim(company)
from layoffs_staging2; 

update layoffs_staging2
set company = trim(company);

select distinct industry
from layoffs_staging2
order by 1; 

select *
from layoffs_staging2
where industry like 'crypto%'; 

update layoffs_staging2
set industry = 'crypto'
where industry  like 'crypto%';

select *
from layoffs_staging2;

-- select distinct each data columns and scroll through to find any issue 

select distinct country, trim(Trailing '.' from country) 
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

-- refining the date style from text to date (use case i.e for time series visualization of data)

select `date`,
str_to_date(`date`, '%m/%d/%Y' )
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y' );

-- changing the data type date of the table 

alter table layoffs_staging2
modify column `date` date;

-- 3 Null and blank Values

select *
from layoffs_staging2
where total_laid_off is null
AND percentage_laid_off is null ;


select *
from layoffs_staging2
where industry is null
or industry = '';

select *
from layoffs_staging2
where total_laid_off IS NULL
and percentage_laid_off is null ;

update layoffs_staging2
set industry = null
where industry = '';

select *
from layoffs_staging2
where industry is null
or industry = '';

select *
from layoffs_staging2
where company like 'Bally%' ;

select t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;


update layoffs_staging2  t1
join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null 
and t2.industry is not null;

select *
from layoffs_staging2;

-- removing null total laid off and percentage laid off
select *
from layoffs_staging2
where total_laid_off IS NULL
and percentage_laid_off is null ;

delete 
from layoffs_staging2
where total_laid_off IS NULL
and percentage_laid_off is null ;

select *
from layoffs_staging2;

-- dropping row num column 
alter table layoffs_staging2
drop column row_num;




