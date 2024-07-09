-- SQL Project - Exploratory Data Analysis
-- Dataset from: https://www.kaggle.com/datasets/swaptr/layoffs-2022


-- Exploring the data and finding trends or patterns or anything interesting like outliers
SELECT *
FROM world_layoffs.layoffs_staging2;

-- Looking at Percentage to see how big these layoffs were
SELECT MAX(percentage_laid_off),  MIN(percentage_laid_off)
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off IS NOT NULL;

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM world_layoffs.layoffs_staging2;

-- Which companies had 1 which is basically 100 percent of they company laid off,
-- ordering by funcs_raised_millions to see how big some of these companies were
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- determining timeline
SELECT MIN(`date`), MAX(`date`)
FROM world_layoffs.layoffs_staging2;

-- companies with the most layoffs
SELECT company, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY company
ORDER BY 2 DESC
LIMIT 10;

-- by location
SELECT location, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;

-- by industry
SELECT industry, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- by country
SELECT country, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

-- by year
SELECT YEAR(`date`), SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

-- by stage
SELECT stage, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;



-- Looking at companies with the most layoffs per year
WITH Company_Year (company, years, total_laid_off) AS 
(
	SELECT company, YEAR(`date`), SUM(total_laid_off)
	FROM world_layoffs.layoffs_staging2
	GROUP BY company, YEAR(`date`)
), 
Company_Year_Rank AS
(
	SELECT *, DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
	FROM Company_Year
	WHERE years IS NOT NULL
)
SELECT * 
FROM Company_Year_Rank
WHERE ranking <= 3
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;



-- Rolling Total of Layoffs Per Month
SELECT SUBSTRING(`date`,1,7) AS dates, SUM(total_laid_off) AS total_laid_off
FROM world_layoffs.layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY dates
ORDER BY dates ASC;

-- using it as a CTE to query off of it
WITH rolling AS 
(
	SELECT SUBSTRING(`date`,1,7) AS dates, SUM(total_laid_off) AS total_laid_off
	FROM world_layoffs.layoffs_staging2
	WHERE SUBSTRING(`date`,1,7) IS NOT NULL
	GROUP BY dates
	ORDER BY dates ASC
)
SELECT dates, SUM(total_laid_off) OVER(ORDER BY dates) AS rolling_total
FROM rolling
ORDER BY dates ASC;

