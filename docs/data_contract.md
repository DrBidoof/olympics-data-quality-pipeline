Dataset: Olympics (Countries + Summer)

Tables: countries.csv, summer.csv

Key relationship: summer.Code → countries.Code

Countries rules

Country: required, unique

Code: required, unique, regex ^[A-Z]{3}$

Population: optional, if present > 0

GDP per Capita: optional, if present > 0; missing values flagged

Summer rules

Year: required, 1896–current year

Code: required, regex ^[A-Z]{3}$, must exist in countries.Code

Medal: required, one of Gold/Silver/Bronze

Athlete: required

Gender: required, allowed set you define