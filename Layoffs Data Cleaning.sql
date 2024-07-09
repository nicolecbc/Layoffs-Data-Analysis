-- SQL Project - Data Cleaning
-- Dataset from: https://www.kaggle.com/datasets/swaptr/layoffs-2022


SELECT * 
FROM world_layoffs.layoffs;

-- Creating a staging table to work in and clean the data. This way we don't alter the table with the raw data
CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

INSERT layoffs_staging 
SELECT * FROM world_layoffs.layoffs;

-- Check the following:
-- 1. Remove duplicates
-- 2. Standardize the data
-- 3. Check null values
-- 4. Remove any unecessary columns


-- *********************************************************************
-- 1. Remove duplicates
-- Checking for duplicates
SELECT *
FROM world_layoffs.layoffs_staging;

WITH duplicate_cte AS (
	SELECT *,
		ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) 
		AS row_num
	FROM world_layoffs.layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;
-- Spot checking
SELECT * 
FROM world_layoffs.layoffs_staging
WHERE company='Casper';

-- Adding the row_num column to a new staging table with to delete all entries with row_num > 1
-- Creating the table
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
);

-- Inserting values into the table created
INSERT INTO `world_layoffs`.`layoffs_staging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT
`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
ROW_NUMBER() OVER(
	PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) 
	AS row_num
FROM world_layoffs.layoffs_staging;

-- Deleting rows with row_num of 2 or greater as these are duplicates
DELETE
FROM world_layoffs.layoffs_staging2
WHERE row_num > 1;


-- *********************************************************************
-- 2. Standardize data
-- Looking at the industry column
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

-- It looks like there are some null and empty rows
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- Taking a look at some individual industries
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'Bally%';
-- Nothing wrong with Bally

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'airbnb%';
-- It looks like airbnb is a travel industry, but one entry just isn't populated.
-- The same thing could be happening to the rest of the blank or null entries
-- So to fix this, here's a query that if there is another row with the same company name
-- it will update it to the non-null industry values.
-- This makes it easy so if there were thousands we wouldn't have to manually check them all

-- Setting blanks to nulls since those are typically easier to work with
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Now if we check those are all null
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- Populating those nulls if possible
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- Checking for null industry entries
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;
-- It looks like Bally's was the only one without another industry populated row, therefore the industry is unknown

-- -----------------------------------------------------------
-- I also noticed that Crypto has different name variations
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

-- Standardizing so that they are all the same name
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- Check
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

-- -------------------------------------------------
-- Now looking at country
SELECT * 
FROM world_layoffs.layoffs_staging2;

-- Removing whitespaces in front of some company names
UPDATE layoffs_staging2
SET company = trim(company); 

-- There are some "United States" and some "United States." with a period at the end. 
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;
-- Let's standardize this.
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);
-- Check
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;

-- ----------------------------------------------------------
-- Fixing date columns
SELECT *
FROM world_layoffs.layoffs_staging2;

-- Using str to date to update this field
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Converting the data type properly
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- *********************************************************************
-- 3. Check null values
-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. 
-- everything looks good, there isn't anything to change with the null values

-- *********************************************************************
-- 4. Remove any unecessary columns
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Deleting useless data 
DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Final check 
SELECT * 
FROM world_layoffs.layoffs_staging2;