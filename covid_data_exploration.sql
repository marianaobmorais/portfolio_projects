-- COVID-19 Data Exploration 
-- Skills used: Joins, Windows Functions, Aggregate Functions, Creating Views, Converting Data Types

-- Software: PostgreSQL

-- Datasets: covid_deaths.csv and covid_vaccination.csv


-- View the datasets

SELECT 
	*
FROM
	covid_deaths
ORDER BY
	location
	,date
LIMIT 1000;


-- and

SELECT
	*
FROM
	covid_vaccination
ORDER BY
	location
	,date
LIMIT 1000;



------------------------
--INFECTION AND DEATHS--
------------------------

-- 1. Population vs Total cases rate and Total cases vs Total deaths rate
-- Likelihood of being infected and likelihood of dying once infected by country

SELECT
	continent
	,location
	,date
	,population		-- (that's not precise bc the population number should have changed, but for the sake of this case study, I will consider the info from this data set)
	,total_cases
	,total_deaths
	,(CAST(total_cases AS NUMERIC)/ CAST(population AS NUMERIC))*100 AS infection_percentage
	,(CAST(total_deaths AS NUMERIC)/ CAST(total_cases AS NUMERIC))*100 AS deaths_percentage
FROM
	covid_deaths
WHERE
	continent IS NOT NULL
ORDER BY
	location,
	date;



-- 2. Daily new cases and new deaths per country
-- Shows the number of new cases and new deaths from January 2020 to August 2023

SELECT
	continent
	,location
	,date
	,population
	,new_cases
	,SUM(new_cases) 
		OVER (PARTITION BY location ORDER BY location, date) AS rolling_new_cases
	,new_deaths
	,SUM(new_deaths)
		OVER (PARTITION BY location ORDER BY location, date) AS rolling_new_deaths
FROM
	covid_deaths
WHERE
	continent IS NOT NULL


-- 3. Difference between cases and deaths in the years of 2020, 2021 and 2022
-- Shows a side-by-side comparison of how the infection and deaths numbers changed between January 2020 and December 2022 by country

SELECT 
	year_2020.continent
	,year_2020.location
	,to_char(year_2020.date, 'MM-DD') AS month_day
	,SUM(year_2020.new_cases) 					
		OVER (PARTITION by year_2020.location ORDER BY year_2020.location, year_2020.date) AS rolling_new_cases_2020	-- Partition by location and date so that the rolling sums of new_cases are separated by distinct locations
	,SUM(year_2020.new_deaths) 
		OVER (PARTITION by year_2020.location ORDER BY year_2020.location, year_2020.date) AS rolling_new_deaths_2020
	--,year_2021.date
	,year_2021.rolling_new_cases_2021
	,year_2021.rolling_new_deaths_2021
	--,year_2022.date
	,year_2022.rolling_new_cases_2022
	,year_2022.rolling_new_deaths_2022
FROM
	covid_deaths AS year_2020
JOIN
	(
	SELECT
		continent
		,location
		,date
		,population
		,new_cases
		,SUM(new_cases) 
			OVER (PARTITION BY location ORDER BY location, date) AS rolling_new_cases_2021
		,SUM(new_deaths)
			OVER (PARTITION BY location ORDER BY location, date) AS rolling_new_deaths_2021
	FROM
		covid_deaths
	WHERE
		continent IS NOT NULL
		AND
		date >= '2021-01-01' AND date <= '2021-12-31'
	) AS year_2021
ON
	year_2020.location = year_2021.location
	AND
	EXTRACT (MONTH FROM year_2020.date) = EXTRACT (MONTH FROM year_2021.date)
	AND
	EXTRACT (DAY FROM year_2020.date) = EXTRACT (DAY FROM year_2021.date)
JOIN
	(
	SELECT 
		continent
		,location
		,date
		,population
		,new_cases
		,SUM(new_cases) 
			OVER (PARTITION BY location ORDER BY location, date) AS rolling_new_cases_2022
		,SUM(new_deaths) 
			OVER (PARTITION BY location ORDER BY location, date) AS rolling_new_deaths_2022
	FROM
		covid_deaths
	WHERE
		continent IS NOT NULL
		AND
		date >= '2022-01-01' AND date <= '2022-12-31'
	) AS year_2022
ON
	year_2021.location = year_2022.location
	AND
	EXTRACT (MONTH FROM year_2020.date) = EXTRACT (MONTH FROM year_2022.date)
	AND
	EXTRACT (DAY FROM year_2020.date) = EXTRACT (DAY FROM year_2022.date)
WHERE
	year_2020.continent IS NOT NULL
	AND
	year_2020.date >= '2020-01-01' AND year_2020.date <= '2020-12-31'
ORDER BY
	location
	,year_2020.date;



-- 4. Total number of deaths per continent
-- Shows total death count per continent

SELECT
	continent
	,SUM(total_population) AS total_population
	,SUM(total_deaths) AS total_deaths_per_continent
FROM
	(
	SELECT
		continent
		,location
		,MAX(population) AS total_population
		,MAX(total_deaths) AS total_deaths
	FROM
		covid_deaths
	WHERE
		continent IS NOT NULL
	GROUP BY
		continent
		,location
	ORDER BY
		continent
	) AS total_deaths_country
GROUP BY
	continent
ORDER BY
	total_deaths_per_continent DESC;
		


-- 5. Death percentage per continent
-- Shows likelihood of dying once infected by continent

SELECT
	continent
	,SUM(total_deaths_country) AS total_deaths_global
	,SUM(total_cases_country) AS total_cases_global
	,(SUM(total_deaths_country)/SUM(total_cases_country))*100 AS death_global_rate
FROM
	(						
	SELECT
		continent
		,location
		,MAX(total_deaths) AS total_deaths_country
		,MAX(total_cases) AS total_cases_country
	FROM
		covid_deaths
	WHERE
		continent IS NOT NULL
	GROUP BY
		continent
		,location
	ORDER BY
		continent
	) AS total_per_country
GROUP BY
	continent;



-----------------
-- VACCINATION --
-----------------
	
-- 6. Rolling vaccination vs population rate
-- Shows percentage of population that has recieved at least one COVID-19 vaccine

SELECT
	continent
	,location
	,date
	,population
	,new_vaccinations
	,rolling_vaccinations_per_country
	,(rolling_vaccinations_per_country/population)*100 AS vaccination_rate
FROM
	(						-- rolling vaccination per country
	SELECT 
		dea.continent
		,dea.location
		,dea.date
		,dea.population
		,vac.new_vaccinations 			-- not every country registered new vaccinations, resulting in null values
		,SUM(vac.new_vaccinations) 
			OVER (PARTITION by dea.location ORDER BY dea.location, dea.date) AS rolling_vaccinations_per_country
	FROM
		covid_deaths AS dea
	JOIN
		covid_vaccination AS vac
		ON
		dea.location = vac.location
		AND
		dea.date = vac.date
	WHERE
		dea.continent IS NOT NULL
	ORDER BY
		dea.location
		,dea.date
	) AS rolling_vaccinations;



-- 7. Population, Total cases, Total deaths and Vaccination rates
-- Shows the changes in infection rate and death rate after vaccination started

SELECT
	dea.continent
	,dea.location
	,dea.date
	,dea.population
	,dea.total_cases
	,dea.total_deaths
	,vac.total_vaccinations
	,(CAST(dea.total_cases AS NUMERIC)/ CAST(dea.population AS NUMERIC))*100 AS infection_percentage
	,(CAST(vac.total_vaccinations AS NUMERIC)/dea.population)*100 AS vaccination_percentage
	,(CAST(dea.total_deaths AS NUMERIC)/ CAST(dea.total_cases AS NUMERIC))*100 AS deaths_per_case_percentage
FROM
	covid_deaths AS dea
JOIN
	(
	SELECT
	 	continent
		,location
		,date
		,total_vaccinations
	 FROM
	 	covid_vaccination
	WHERE
		continent IS NOT NULL
	ORDER BY
		location
		,date
	) AS vac
ON
	dea.location = vac.location
	AND
	dea.date = vac.date
WHERE
	dea.continent IS NOT NULL
ORDER BY
	dea.location
	,dea.date;
	


--------------------------------------------------
-- Create view to store data for visualizations --
--------------------------------------------------

-- 

CREATE VIEW population_total_cases_total_deaths AS
SELECT
	continent
	,location
	,date
	,population
	,total_cases
	,total_deaths
	,(CAST(total_cases AS NUMERIC)/ CAST(population AS NUMERIC))*100 AS infection_percentage
	,(CAST(total_deaths AS NUMERIC)/ CAST(total_cases AS NUMERIC))*100 AS deaths_percentage
FROM
	covid_deaths
WHERE
	continent IS NOT NULL
ORDER BY
	location,
	date;


	
-- 

CREATE VIEW cases_deaths_20_21_20 AS
SELECT 
	year_2020.continent
	,year_2020.location
	,to_char(year_2020.date, 'MM-DD') AS month_day
	,SUM(year_2020.new_cases) 
		OVER (PARTITION by year_2020.location ORDER BY year_2020.location, year_2020.date) AS rolling_new_cases_2020	-- Partition by location and date so that the rolling sums of new_cases are separated by distinct locations
	,SUM(year_2020.new_deaths) 
		OVER (PARTITION by year_2020.location ORDER BY year_2020.location, year_2020.date) AS rolling_new_deaths_2020
	--,year_2021.date
	,year_2021.rolling_new_cases_2021
	,year_2021.rolling_new_deaths_2021
	--,year_2022.date
	,year_2022.rolling_new_cases_2022
	,year_2022.rolling_new_deaths_2022
FROM
	covid_deaths AS year_2020
JOIN
	(
	SELECT
		continent
		,location
		,date
		,population
		,new_cases
		,SUM(new_cases) 
			OVER (PARTITION BY location ORDER BY location, date) AS rolling_new_cases_2021
		,SUM(new_deaths)
			OVER (PARTITION BY location ORDER BY location, date) AS rolling_new_deaths_2021
	FROM
		covid_deaths
	WHERE
		continent IS NOT NULL
		AND
		date >= '2021-01-01' AND date <= '2021-12-31'
	) AS year_2021
ON
	year_2020.location = year_2021.location
	AND
	EXTRACT (MONTH FROM year_2020.date) = EXTRACT (MONTH FROM year_2021.date)
	AND
	EXTRACT (DAY FROM year_2020.date) = EXTRACT (DAY FROM year_2021.date)
JOIN
	(
	SELECT 
		continent
		,location
		,date
		,population
		,new_cases
		,SUM(new_cases) 
			OVER (PARTITION BY location ORDER BY location, date) AS rolling_new_cases_2022
		,SUM(new_deaths) 
			OVER (PARTITION BY location ORDER BY location, date) AS rolling_new_deaths_2022
	FROM
		covid_deaths
	WHERE
		continent IS NOT NULL
		AND
		date >= '2022-01-01' AND date <= '2022-12-31'
	) AS year_2022
ON
	year_2021.location = year_2022.location
	AND
	EXTRACT (MONTH FROM year_2020.date) = EXTRACT (MONTH FROM year_2022.date)
	AND
	EXTRACT (DAY FROM year_2020.date) = EXTRACT (DAY FROM year_2022.date)
WHERE
	year_2020.continent IS NOT NULL
	AND
	year_2020.date >= '2020-01-01' AND year_2020.date <= '2020-12-31'
ORDER BY
	location
	,year_2020.date;
	
	
	
-- 

CREATE VIEW death_percentage_continent AS
SELECT
	continent
	,SUM(total_deaths_country) AS total_deaths_global
	,SUM(total_cases_country) AS total_cases_global
	,(SUM(total_deaths_country)/SUM(total_cases_country))*100 AS death_global_rate
FROM
	(
	SELECT
		continent
		,location
		,MAX(total_deaths) AS total_deaths_country
		,MAX(total_cases) AS total_cases_country
	FROM
		covid_deaths
	WHERE
		continent IS NOT NULL
	GROUP BY
		continent
		,location
	ORDER BY
		continent
	) AS total_per_country
GROUP BY
	continent;



-- 

CREATE VIEW vaccination_rate AS
SELECT
	continent
	,location
	,date
	,population
	,new_vaccinations
	,rolling_vaccinations_per_country
	,(rolling_vaccinations_per_country/population)*100 AS vaccination_rate
FROM
	(
	SELECT 
		dea.continent
		,dea.location
		,dea.date
		,dea.population
		,vac.new_vaccinations 			--not every country registered new vaccinations, resulting in null values
		,SUM(vac.new_vaccinations) 
			OVER (PARTITION by dea.location ORDER BY dea.location, dea.date) AS rolling_vaccinations_per_country
	FROM
		covid_deaths AS dea
	JOIN
		covid_vaccination AS vac
		ON
		dea.location = vac.location
		AND
		dea.date = vac.date
	WHERE
		dea.continent IS NOT NULL
	ORDER BY
		dea.location
		,dea.date
	) AS rolling_vaccinations

	
