create table covid_analysis.covid_deaths
(iso_code varchar (10),
 continent varchar (30),
location varchar(60),
date date,
population bigint,
total_cases bigint,
new_cases bigint,
new_cases_smoothed decimal,
total_deaths bigint,
new_deaths bigint,
new_deaths_smoothed decimal,
total_cases_per_million decimal,
new_cases_per_million decimal,
new_cases_smoothed_per_million decimal,
total_deaths_per_million decimal,
new_deaths_per_million decimal,
new_deaths_smoothed_per_million decimal)

select * from covid_analysis.covid_deaths
order by 3,4

create table covid_analysis.covid_vaccinations
( iso_code varchar (10),
 continent varchar (30),
 location varchar (60),
 date date,
stringency_index decimal,
 population_density decimal,
median_age decimal,
aged_65_older decimal,
aged_70_older decimal,
gdp_per_capita decimal,
extreme_poverty decimal,
 cardiovasc_death_rate decimal,
diabetes_prevalence decimal,
handwashing_facilities decimal,
hospital_beds_per_thousand decimal,
life_expectancy decimal,
human_development_index decimal,
new_vaccinations bigint,
new_vaccinations_smoothed bigint,
total_vaccinations_per_hundred decimal,
people_vaccinated_per_hundred decimal)

select *  from covid_analysis.covid_vaccinations

---Continents available in the dataset with location count---
select continent,
count(location) as location_count
from covid_analysis.covid_deaths
group by continent

---Global Numbers---
select continent,
count(total_cases) as total_number_of_cases
from covid_analysis.covid_deaths
group by continent 

---Global percentage---
select
sum(new_cases) as global_total,
sum(cast(new_deaths as int)) as total_deaths,
sum(cast(new_deaths as int))/sum(new_cases)*100 as deathpercentage
from covid_analysis.covid_deaths

---Total population vs vaccinations---
select
covid_deaths.continent,
covid_deaths.location,
covid_deaths.date,
covid_deaths.population,
covid_vaccinations.new_vaccinations
FROM covid_analysis.covid_deaths
inner join  covid_analysis.covid_vaccinations
on covid_deaths.location = covid_vaccinations.location
and covid_deaths.date = covid_vaccinations.date
order by date

--Use CTE to perform calculation on partition in previous query---
with popvsvac (Continent, Location, Date, Population, New_vaccinations,
RollingPeopleVaccination)
as 
(select 
covid_deaths.continent,
covid_deaths.location,
covid_deaths.date,
covid_deaths.population,
covid_vaccinations.new_vaccinations,
sum( covid_vaccinations.new_vaccinations  ::numeric)
over (partition by covid_deaths.location order by covid_deaths.location , 
covid_deaths.date) as rollingpeoplevaccinated
FROM covid_analysis.covid_deaths
inner join  covid_analysis.covid_vaccinations
on covid_deaths.location = covid_vaccinations.location
and covid_deaths.date = covid_vaccinations.date)
select * , (popvsvac.rollingpeoplevaccination/population)*100 as percentage
from popvsvac

---Using Temp Table to perform partition calculation in previous query---
create table percentpopulationvaccinated
(continent varchar (255),
location varchar (255),
date date,
population numeric,
new_vaccinations numeric,
rollingpeoplevaccinated numeric)

insert into percentpopulationvaccinated
select 
covid_deaths.continent,
covid_deaths.location,
covid_deaths.date,
covid_deaths.population,
covid_vaccinations.new_vaccinations,
sum( covid_vaccinations.new_vaccinations  ::numeric)
over (partition by covid_deaths.location order by covid_deaths.location , 
covid_deaths.date) as rollingpeoplevaccinated
FROM covid_analysis.covid_deaths
inner join  covid_analysis.covid_vaccinations
on covid_deaths.location = covid_vaccinations.location
and covid_deaths.date = covid_vaccinations.date


select * , (popvsvac.rollingpeoplevaccination/population)*100
from percentpopulationvaccinated

---creating view---

create view  percentpeoplevaccinated as
select 
covid_deaths.continent,
covid_deaths.location,
covid_deaths.date,
covid_deaths.population,
covid_vaccinations.new_vaccinations,
sum( covid_vaccinations.new_vaccinations  ::numeric)
over (partition by covid_deaths.location order by covid_deaths.location , 
covid_deaths.date) as rollingpeoplevaccinated
FROM covid_analysis.covid_deaths
inner join  covid_analysis.covid_vaccinations
on covid_deaths.location = covid_vaccinations.location
and covid_deaths.date = covid_vaccinations.date
select * , (popvsvac.rollingpeoplevaccination/population)*100
from popvsvac

select * from percentagepeoplevaccinated;



----Showing results from Africa

---Showing the death percentage per country---
select
continent,location, 
date, total_cases, total_deaths,
(total_deaths ::numeric  / total_cases)*100  as deathpercentage
from covid_analysis.covid_deaths
where continent = 'Africa'
order by 1,2

------------
create view deathpercentage_v as
select
continent,location, 
date, total_cases, total_deaths,
(total_deaths ::numeric  / total_cases)*100  as deathpercentage
from covid_analysis.covid_deaths
where continent = 'Africa'
order by 1,2

select * from deathpercentage_v


---Percentage of population that has contracted covid---
select continent, location, date,
total_cases, population,
(total_cases ::numeric / population)*100 as population_death_percentage
from covid_analysis.covid_deaths
where continent = 'Africa'
order by date


---Locations with highest infection rate compared with population---
select 
location, population,
max(total_cases) as HighestInfectionCount,
max(total_cases ::numeric/population)*100 as "percentage population infected"
from covid_analysis.covid_deaths
where continent like '%Africa%'
group by location, population
order by 1 desc

---Countries with highest death rate count per population in Africa---
select 
location, population,
max(total_deaths) as death_count,
max(total_deaths :: numeric / population)*100 as total_death_rate
from covid_analysis.covid_deaths
where continent like '%Africa%' 
group by location, population
order by 1 desc

---Total numbers with deathpercentage---
select
sum(new_cases) as continent_total,
sum(cast(new_deaths as int)) as total_deaths,
sum(cast(new_deaths as int))/sum(new_cases)*100 as deathpercentage
from covid_analysis.covid_deaths
where continent = 'Africa'


select * from covid_analysis.covid_vaccinations
---Total number of persons vaccinated per country---
select location,
sum(new_vaccinations) 
from covid_analysis.covid_vaccinations
where continent = 'Africa'
group by location




