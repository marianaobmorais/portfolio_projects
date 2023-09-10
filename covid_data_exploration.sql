-- data source:

-- business task:
--
--


-- Data exploration
-- datasets: covid_deaths and covid_vaccination
-- Select the data
SELECT 
	*
FROM
	covid_deaths
ORDER BY
	location,
	date
LIMIT 1000;



-- Population vs Total cases and Total cases vs Total deaths
-- Likelihood of being infected and likelihood of dying once infected
-- (that's not precise bc the population number should have changed)

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


	
-- Difference between cases and deaths in the years of 2020, 2021 and 2022
-- How the infection and deaths numbers changed between January2020 and December 2022
SELECT 
	year_2020.continent
	,year_2020.location
	,to_char(year_2020.date, 'MM-DD') AS month_day
	,SUM(year_2020.new_cases) 
		OVER (PARTITION by year_2020.location ORDER BY year_2020.location, year_2020.date) AS rolling_new_cases_2020
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


-- absolute death count per country (already answered two queries above)
SELECT
	continent
	,location
	,MAX(total_deaths) AS total_deaths
FROM
	covid_deaths
WHERE
	continent IS NOT NULL
	AND
	total_deaths IS NOT NULL
GROUP BY
	continent
	,location
ORDER BY
	total_deaths DESC;
	
	
-- rolling deaths per country (maybe it's not important right now)
SELECT 
	continent
	,location
	,date
	,population
	,new_deaths 			
	,SUM(new_deaths) 
		OVER (PARTITION by location ORDER BY location, date) AS rolling_deaths_per_country
FROM
	covid_deaths 
WHERE
	continent IS NOT NULL
ORDER BY
	location
	,date;



-- total deaths per continent
SELECT
	continent
	,SUM(total_deaths) AS total_deaths_per_continent
FROM
	(
	SELECT
		continent
		,location
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
		
		

-- death percentage per continent
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
	
-- exploring the dataset covid_vaccination
-- select the data
SELECT
	*
FROM
	covid_vaccination
LIMIT 1000;


	
-- rolling new vaccination vs population rate
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
	
	

-- total vaccination vs total population rate (the query above already answers this)
--SELECT
--	location
--	,population
--	,COALESCE(MAX(rolling_vaccinations_per_country), MAX(total_vaccinations)) AS total_vaccinations  --coalesce because some countries has null for the MAX(rolling_vaccinations_per_country) 
--	,(COALESCE(MAX(rolling_vaccinations_per_country), MAX(total_vaccinations))/population)*100 AS vaccination_rate
--FROM
--	(
--	SELECT 
--		dea.continent 
--		,dea.location
--		,dea.date
--		,dea.population
--		,vac.total_vaccinations
--		,vac.new_vaccinations
--		,SUM(vac.new_vaccinations) 
--			OVER (PARTITION by dea.location ORDER BY dea.location, dea.date) AS rolling_vaccinations_per_country
--	FROM
--		covid_deaths AS dea
--	JOIN
--		covid_vaccination AS vac
--		ON
--		dea.location = vac.location
--		AND
--		dea.date = vac.date
--	WHERE
--		dea.continent IS NOT NULL
--	ORDER BY
--		dea.location
--		,dea.date
--	) AS rolling_vaccinations
--WHERE
--	total_vaccinations IS NOT NULL
--GROUP BY
--	location
--	,population
--ORDER BY
--	vaccination_rate DESC;



-- Create view to store data for visualizations

-- Population vs Total cases and Total cases vs Total deaths
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

	
-- Difference between cases and deaths in the years of 2020, 2021 and 2022
CREATE VIEW cases_deaths_20_21_20 AS
SELECT 
	year_2020.continent
	,year_2020.location
	,to_char(year_2020.date, 'MM-DD') AS month_day
	,SUM(year_2020.new_cases) 
		OVER (PARTITION by year_2020.location ORDER BY year_2020.location, year_2020.date) AS rolling_new_cases_2020
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
	
	
	
-- Death percentage per continent 
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

-- Rolling new vaccination vs population rate
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
	
