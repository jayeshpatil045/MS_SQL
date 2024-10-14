/*'''

@Author: Jayesh Patil 
@Date: 2024-09-29 
@Last Modified by: Jayesh Patil 
@Title: Covid data set problem 

'''
*/
CREATE DATABASE covid
USE covid

CREATE TABLE covid_19_data (
    province_state VARCHAR(255),  -- Adjust as necessary
    country_region VARCHAR(255) NOT NULL,
    latitude DECIMAL(10, 7),
    longitude DECIMAL(10, 7),
    date DATE NOT NULL,
    confirmed INT,
    deaths INT,
    recovered INT,
    active INT,
    who_region VARCHAR(1000),
);
ALTER TABLE covid_19_data
ALTER COLUMN confirmed BIGINT;

ALTER TABLE covid_19_data
ALTER COLUMN deaths BIGINT;

ALTER TABLE covid_19_data
ALTER COLUMN recovered BIGINT;

ALTER TABLE covid_19_data
ALTER COLUMN active BIGINT;

BULK INSERT covid_19_data
FROM 'C:\Covid_data_set\covid_19_clean_complete.csv'
WITH (
    FIELDTERMINATOR = ',',  
    ROWTERMINATOR = '\n', 
    FIRSTROW = 2,           
    TABLOCK
);
SELECT * FROM covid_19_data

CREATE TABLE worldometer_data(
    country_region VARCHAR(100),
    continent VARCHAR(50),
    population BIGINT,
    total_cases BIGINT,
    new_cases BIGINT,
    total_deaths BIGINT,
    new_deaths BIGINT,
    total_recovered BIGINT,
    new_recovered BIGINT,
    active_cases BIGINT,
    serious_critical INTEGER,
    tot_cases_per_million FLOAT,
    deaths_per_million FLOAT,
    total_tests BIGINT,
    tests_per_million FLOAT,
    who_region VARCHAR(50)
);
BULK INSERT worldometer_data
FROM 'C:\Covid_data_set\worldometer_data.csv'
WITH (
    FIELDTERMINATOR = ',',  
    ROWTERMINATOR = '\n', 
    FIRSTROW = 2,           
    TABLOCK
);
SELECT * FROM worldometer_data

--CREATE TABLE covid_vaccine_state(
--    updated_on DATE,
--    state VARCHAR(100),
--    total_doses_administered BIGINT,
--    sessions BIGINT,
--    sites BIGINT,
--    first_dose_administered BIGINT,
--    second_dose_administered BIGINT,
--    male_doses_administered BIGINT,
--    female_doses_administered BIGINT,
--    transgender_doses_administered BIGINT,
--    covaxin_doses_administered BIGINT,
--    covishield_doses_administered BIGINT,
--    sputnik_v_doses_administered BIGINT,
--    aefi BIGINT,
--    age_18_to_44_years_doses_administered BIGINT,
--    age_45_to_60_years_doses_administered BIGINT,
--    age_60_plus_years_doses_administered BIGINT,
--    age_18_to_44_years_individuals_vaccinated BIGINT,
--    age_45_to_60_years_individuals_vaccinated BIGINT,
--    age_60_plus_years_individuals_vaccinated BIGINT,
--    male_individuals_vaccinated BIGINT,
--    female_individuals_vaccinated BIGINT,
--    transgender_individuals_vaccinated BIGINT,
--    total_individuals_vaccinated BIGINT
--);

--BULK INSERT covid_vaccine_state
--FROM 'C:\Covid_data_set\covid_vaccine_statewise.csv'
--WITH (
--    FIELDTERMINATOR = ',',  
--    ROWTERMINATOR = '\r\n', 
--    FIRSTROW = 2,           
--    TABLOCK
--);

--1.To find out the death percentage locally and globally
--A.Global Death Percentage
SELECT 
	SUM(deaths) AS global_deaths,
	SUM(deaths)*100.0 / SUM(confirmed) AS global_death_percentage
FROM covid_19_data WHERE confirmed >0
--B.local Death Percentage by country
SELECT 
    country_region,
	SUM(deaths) AS local_deaths,
	SUM(deaths)*100.0 / SUM(confirmed) AS local_death_percentage
FROM covid_19_data WHERE confirmed >0
GROUP BY country_region

--2. To find out the infected population percentage locally and globally
--A.Global Infected Population Percentage
SELECT 
    SUM(c.confirmed) AS total_global_cases,
    SUM(W.population) AS total_global_population,
    (SUM(c.confirmed) * 100.0 / SUM(W.population)) AS global_infected_population_percentage
FROM 
    worldometer_data W,
	covid_19_data c
WHERE 
    c.confirmed > 0;


--B.Local Infected Population Percentage by Country
-- Local Infected Population Percentage by Country
SELECT 
    W.country_region,
    W.total_cases AS confirmed_cases,
    W.population,
    (W.total_cases * 100.0 / W.population) AS local_infected_population_percentage
FROM 
    worldometer_data W
WHERE 
    W.total_cases > 0;

--3. To find out the countries with the highest infection rates
SELECT TOP 10
	w.country_region,
	w.total_cases AS confirmed_cases,
	w.population,
	(w.total_cases * 100.0 / w.population) as infection_rate
FROM
	worldometer_data w
WHERE 
    w.total_cases >0
ORDER BY 
    infection_rate DESC

--4. A.To find out the countries with the highest death counts
SELECT 
    country_region,
	SUM(deaths) AS total_deaths
FROM 
    covid_19_data
WHERE
    deaths > 0
GROUP BY
   country_region
ORDER BY
   total_deaths DESC
--B.To find out the continents with the highest death counts
SELECT 
    who_region,
	SUM(deaths) AS total_deaths
FROM 
    covid_19_data
WHERE
    deaths > 0
GROUP BY
   who_region
ORDER BY
   total_deaths DESC
--5. A.Average number of deaths by day (Countries)
SELECT 
	country_region,
    AVG(daily_deaths) AS average_daily_deaths
FROM (
	SELECT 
		country_region,
		date,
		SUM(deaths) AS daily_deaths
	FROM 
       covid_19_data
	GROUP BY 
		country_region, date
) AS daily_data
GROUP BY 
    country_region
ORDER BY 
    average_daily_deaths DESC;
--5. B.Average number of deaths by day (continents)
SELECT 
	who_region,
    AVG(daily_deaths) AS average_daily_deaths
FROM (
	SELECT 
		who_region,
		date,
		SUM(deaths) AS daily_deaths
	FROM 
       covid_19_data
	GROUP BY 
		who_region, date
) AS daily_data
GROUP BY 
    who_region
ORDER BY 
    average_daily_deaths DESC;

--6. Average of cases divided by the number of population of each country (TOP 10)
SELECT TOP 10
    C.country_region,
    (SUM(C.confirmed) * 1.0 / SUM(W.population)) AS average_cases_per_population
FROM 
    covid_19_data C
JOIN 
    worldometer_data W ON C.country_region = W.country_region
GROUP BY 
    C.country_region
HAVING 
    SUM(C.confirmed) > 0  
ORDER BY 
    average_cases_per_population DESC;

--7. Considering the highest value of total cases, which countries have the highest rate of infection in relation to population?

SELECT 
    c.country_region,
    SUM(c.confirmed) AS TotalCases,
    (SUM(c.Confirmed) * 100.0) / w.Population AS InfectionRate
FROM covid_19_data c
JOIN worldometer_data w ON c.country_region = w.country_region
GROUP BY c.country_region, w.Population
ORDER BY TotalCases DESC;

--Using JOINS to combine the covid_deaths and covid_vaccine tables :
--1. To find out the population vs the number of people vaccinated
SELECT * FROM covid_vaccine_statewise_new
SELECT top 1
    W.country_region,
    W.population AS total_population,
    VS.total_individuals_vaccinated AS vaccinated_population,
    (VS.total_individuals_vaccinated * 100.0 / W.population) AS vaccinated_percentage
FROM 
    worldometer_data W
JOIN 
    covid_vaccine_statewise_new VS ON W.country_region = VS.state
ORDER BY 
    vaccinated_percentage DESC;

--2. To find out the percentage of different vaccine taken by people in a country
SELECT 
 w.country_region AS country,
 w.population,
 MAX(s.total_doses_administered) AS total_doses_administrated, 
 MAX(s.Covaxin_doses_administered) AS Covaxin_doses_administered,
 MAX(s.sputnik_v_doses_administered) AS SputnikV_doses_administered,
 MAX(s.CoviShield_doses_administered) AS CoviShield_doses_administered,
 MAX(s.Covaxin_doses_administered) * 100.0 / (w.population) AS Covaxin_doses_administered_percentage,
 MAX(s.sputnik_v_doses_administered) * 100.0 /(w.population) AS SputnikV_doses_administered_percentage,
 MAX(s.CoviShield_doses_administered) * 100.0 /(w.population) AS CoviShield_doses_administered_percentage
 FROM worldometer_data AS w
 JOIN
 covid_vaccine_statewise_new AS s
 ON w.country_region = s.state
GROUP BY w.population,w.country_region
-- 3. To find out percentage of people who took both the doses
SELECT 
 w.country_region AS country,
 w.population,
 MAX(s.second_dose_administered) AS both_dose_administrated,
 MAX(s.second_dose_administered) * 100.0 / w.population AS both_dose_administrated_percentage
 FROM worldometer_data AS w
 JOIN
 covid_vaccine_statewise AS s
 ON w.country_region = s.state
GROUP BY w.population,w.country_region




    







	

