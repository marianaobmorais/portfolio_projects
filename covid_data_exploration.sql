-- This case study on COVID-19 was based on Alex Freberg’s Data Analyst Portfolio Project Tutorial for SQL and Tableau.
-- After watching the tutorial, I decided to explore the data on COVID-19 and do my own explorations.
-- In this case study I use notions of windowing functions, rolling numbers, extract from date, subqueries, aggregation functions.

-- Business task:
-- Vaccination saves lives: how the rise of vaccination decreased the number of infections and deaths by COVID-19.
-- It is expected that the rise of vaccination will decrease the infection and death rates.

-- Data source: COVID-19 Data Explorer (WHO COVID-19 Dashboard. Geneva: World Health Organization, 2020, dataset downloaded through ourworldindata.org) contains raw data on confirmed COIVD-19 cases and deaths worldwide.
-- The data includes data on confirmed cases, deaths, hospitalizations, and testing. This dataset was generated by WHO COVID-19  Dashboard between January, 3rd 2020 and August, 30th 2023.
-- Datasets: I split the COVID-19 dataset into 2 CSV files in order to perform JOIN functions using SQL: covid_deaths.csv and covid_vaccination.csv


-- Software: PostgreSQL


-- View datasets
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


--####################
--INFECTION AND DEATHS
--####################

-- 1. Worldwide: Population vs Total cases and Total cases vs Total deaths
-- Likelihood of being infected and likelihood of dying once infected
-- (that's not precise bc the population number should have changed, but for the sake of this case study, I will consider the info from this data set)
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

	
-- 2. Worldwide: Difference between cases and deaths in the years of 2020, 2021 and 2022
-- How the infection and deaths numbers changed between January 2020 and December 2022
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



-- 3. Continentwide: Total number of deaths per continent
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
		


-- 4. Continentwide: Death percentage per continent
-- Percentage of deaths per cases
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


--################
--VACCINATION RATE
--################
	
-- 5. Worldwide: Rolling vaccination vs population rate
-- Percentage of vaccinated people
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

-- 6. Worldwide: Population, Total cases, Total deaths and Total vaccination rates
-- The changes in infection rate and death rate after vaccination started
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
	

--#############################################
-- Create view to store data for visualizations
--#############################################

-- Worldwide: Population vs Total cases and Total cases vs Total deaths
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

	
-- Worldwide: Difference between cases and deaths in the years of 2020, 2021 and 2022
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
	
	
	
-- Continentwide: Death percentage per continent 
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

-- Worldwide: Rolling vaccination vs population rate
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

	
