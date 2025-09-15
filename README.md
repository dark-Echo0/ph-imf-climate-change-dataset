#  ph-imf-climate-change-dataset

This project was implemented for the purpose of practicing my knowledge in data cleaning. To achieve this, I used the Medallion Architecture, a layered approach for structuring data in a data lake or lakehouse to ensure:
- *Data quality*
- *Reliability*
- *Easy consumption by analytics*

*However, in this project, I only used the Bronze and Silver layers*

The Layer of Medallion Architecture are:
- **Bronze Layer** this stage is for raw ingestion. It stores raw data as-is from source systems including JSON, CSV, logs, and other formats.
- **Silver Layer** this stage is for data cleaning, deduplication, and enrichment. It also ensures a consistent schema and is suitable for analytics pipelines
-  **Gold Layer** this stage is for the final curated tables used in dashboards, business intelligence, or machine learning. It includes aggregations and business metrics and is often used for reporting.





#### Install the required package


```python
pip install sqlalchemy pyodbc pandas
```

    Defaulting to user installation because normal site-packages is not writeable
    Requirement already satisfied: sqlalchemy in c:\programdata\anaconda3\lib\site-packages (2.0.39)
    Requirement already satisfied: pyodbc in c:\programdata\anaconda3\lib\site-packages (5.2.0)
    Requirement already satisfied: pandas in c:\programdata\anaconda3\lib\site-packages (2.2.3)
    Requirement already satisfied: greenlet!=0.4.17 in c:\programdata\anaconda3\lib\site-packages (from sqlalchemy) (3.1.1)
    Requirement already satisfied: typing-extensions>=4.6.0 in c:\programdata\anaconda3\lib\site-packages (from sqlalchemy) (4.12.2)
    Requirement already satisfied: numpy>=1.26.0 in c:\programdata\anaconda3\lib\site-packages (from pandas) (2.1.3)
    Requirement already satisfied: python-dateutil>=2.8.2 in c:\programdata\anaconda3\lib\site-packages (from pandas) (2.9.0.post0)
    Requirement already satisfied: pytz>=2020.1 in c:\programdata\anaconda3\lib\site-packages (from pandas) (2024.1)
    Requirement already satisfied: tzdata>=2022.7 in c:\programdata\anaconda3\lib\site-packages (from pandas) (2025.2)
    Requirement already satisfied: six>=1.5 in c:\programdata\anaconda3\lib\site-packages (from python-dateutil>=2.8.2->pandas) (1.17.0)
    Note: you may need to restart the kernel to use updated packages.
    


```python
import pyodbc
print(pyodbc.drivers())
```

    ['SQL Server', 'SQL Server Native Client RDA 11.0', 'ODBC Driver 17 for SQL Server', 'Microsoft Access Driver (*.mdb, *.accdb)', 'Microsoft Excel Driver (*.xls, *.xlsx, *.xlsm, *.xlsb)', 'Microsoft Access Text Driver (*.txt, *.csv)', 'Microsoft Access dBASE Driver (*.dbf, *.ndx, *.mdx)']
    

#### Import Libraries


```python
from sqlalchemy import create_engine
import pandas as pd

server = 'LAPTOP-LJ0109LP\SQLEXPRESS'      # e.g., 'localhost\SQLEXPRESS'
database = 'PHClimateChangeDataset'

engine = create_engine(
    f"mssql+pyodbc://@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
)

```

#### Query the table from the Bronze layer
The tables is raw from dataset


```python
# Query the 'bronze.ghg_co2_emissions' table
df_co2_emissions_bro = pd.read_sql("SELECT * FROM bronze.ghg_co2_emissions", engine)
df_co2_emissions_bro.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>country</th>
      <th>iso2</th>
      <th>iso3</th>
      <th>indicator_used</th>
      <th>unit</th>
      <th>cts_code</th>
      <th>cts_name</th>
      <th>cts_full_descriptor</th>
      <th>industry</th>
      <th>scale</th>
      <th>...</th>
      <th>2009</th>
      <th>2010</th>
      <th>2011</th>
      <th>2012</th>
      <th>2013</th>
      <th>2014</th>
      <th>2015</th>
      <th>2016</th>
      <th>2017</th>
      <th>2018</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Philippines</td>
      <td>PH</td>
      <td>PHL</td>
      <td>CO2 emissions</td>
      <td>Millions of Metric tons of CO2</td>
      <td>ECNC</td>
      <td>CO2 Emissions</td>
      <td>Environment_ Climate Change_ Economic Activity...</td>
      <td>Accomodation and food services</td>
      <td>Units</td>
      <td>...</td>
      <td>0.324</td>
      <td>0.386</td>
      <td>0.429</td>
      <td>0.446</td>
      <td>0.492</td>
      <td>0.579</td>
      <td>0.54</td>
      <td>0.65</td>
      <td>0.784</td>
      <td>0.852</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Philippines</td>
      <td>PH</td>
      <td>PHL</td>
      <td>CO2 emissions</td>
      <td>Millions of Metric tons of CO2</td>
      <td>ECNC</td>
      <td>CO2 Emissions</td>
      <td>Environment_ Climate Change_ Economic Activity...</td>
      <td>Activities of households as employers_ undiffe...</td>
      <td>Units</td>
      <td>...</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Philippines</td>
      <td>PH</td>
      <td>PHL</td>
      <td>CO2 emissions</td>
      <td>Millions of Metric tons of CO2</td>
      <td>ECNC</td>
      <td>CO2 Emissions</td>
      <td>Environment_ Climate Change_ Economic Activity...</td>
      <td>Administrative and support services</td>
      <td>Units</td>
      <td>...</td>
      <td>0.37</td>
      <td>0.362</td>
      <td>0.313</td>
      <td>0.272</td>
      <td>0.281</td>
      <td>0.34</td>
      <td>0.333</td>
      <td>0.384</td>
      <td>0.481</td>
      <td>0.539</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Philippines</td>
      <td>PH</td>
      <td>PHL</td>
      <td>CO2 emissions</td>
      <td>Millions of Metric tons of CO2</td>
      <td>ECNC</td>
      <td>CO2 Emissions</td>
      <td>Environment_ Climate Change_ Economic Activity...</td>
      <td>Agriculture_ hunting_ forestry</td>
      <td>Units</td>
      <td>...</td>
      <td>0.292</td>
      <td>0.306</td>
      <td>0.314</td>
      <td>0.319</td>
      <td>0.343</td>
      <td>0.335</td>
      <td>0.353</td>
      <td>0.394</td>
      <td>0.471</td>
      <td>0.449</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Philippines</td>
      <td>PH</td>
      <td>PHL</td>
      <td>CO2 emissions</td>
      <td>Millions of Metric tons of CO2</td>
      <td>ECNC</td>
      <td>CO2 Emissions</td>
      <td>Environment_ Climate Change_ Economic Activity...</td>
      <td>Air transport</td>
      <td>Units</td>
      <td>...</td>
      <td>3.74</td>
      <td>3.986</td>
      <td>4.325</td>
      <td>4.538</td>
      <td>4.84</td>
      <td>5.684</td>
      <td>5.692</td>
      <td>6.552</td>
      <td>6.825</td>
      <td>7.199</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 34 columns</p>
</div>




```python
df_co2_emissions_bro.info()
```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 135 entries, 0 to 134
    Data columns (total 34 columns):
     #   Column               Non-Null Count  Dtype 
    ---  ------               --------------  ----- 
     0   country              135 non-null    object
     1   iso2                 135 non-null    object
     2   iso3                 135 non-null    object
     3   indicator_used       135 non-null    object
     4   unit                 135 non-null    object
     5   cts_code             135 non-null    object
     6   cts_name             135 non-null    object
     7   cts_full_descriptor  135 non-null    object
     8   industry             135 non-null    object
     9   scale                135 non-null    object
     10  1995                 135 non-null    object
     11  1996                 135 non-null    object
     12  1997                 135 non-null    object
     13  1998                 135 non-null    object
     14  1999                 135 non-null    object
     15  2000                 135 non-null    object
     16  2001                 135 non-null    object
     17  2002                 135 non-null    object
     18  2003                 135 non-null    object
     19  2004                 135 non-null    object
     20  2005                 135 non-null    object
     21  2006                 135 non-null    object
     22  2007                 135 non-null    object
     23  2008                 135 non-null    object
     24  2009                 135 non-null    object
     25  2010                 135 non-null    object
     26  2011                 135 non-null    object
     27  2012                 135 non-null    object
     28  2013                 135 non-null    object
     29  2014                 135 non-null    object
     30  2015                 135 non-null    object
     31  2016                 135 non-null    object
     32  2017                 135 non-null    object
     33  2018                 135 non-null    object
    dtypes: object(34)
    memory usage: 36.0+ KB
    


```python
# Query the 'bronze.ghg_carbon_footprints' table
df_carbon_footprints_bro = pd.read_sql("SELECT * FROM bronze.ghg_carbon_footprints", engine)
df_carbon_footprints_bro.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>country</th>
      <th>iso2</th>
      <th>iso3</th>
      <th>indicator_used</th>
      <th>unit</th>
      <th>cts_code</th>
      <th>cts_name</th>
      <th>scale</th>
      <th>1995</th>
      <th>1996</th>
      <th>...</th>
      <th>2012</th>
      <th>2013</th>
      <th>2014</th>
      <th>2015</th>
      <th>2016</th>
      <th>2017</th>
      <th>2018</th>
      <th>2019</th>
      <th>2020</th>
      <th>2021</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Philippines</td>
      <td>PH</td>
      <td>PHL</td>
      <td>CO2 Emissions Embodied in Final Demand; balance</td>
      <td>Millions of metric tons</td>
      <td>ECBTDB</td>
      <td>CO2 Emissions Embodied in Final Demand; Of whi...</td>
      <td>Units</td>
      <td>-16.579</td>
      <td>-21.626</td>
      <td>...</td>
      <td>-22.66</td>
      <td>-23.206</td>
      <td>-23.016</td>
      <td>-27.356</td>
      <td>-35.476</td>
      <td>-35.587</td>
      <td>-40.487</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Philippines</td>
      <td>PH</td>
      <td>PHL</td>
      <td>CO2 Emissions Embodied in Final Domestic Demand</td>
      <td>Millions of metric tons</td>
      <td>ECBTDD</td>
      <td>CO2 Emissions Embodied in Final Demand; Of whi...</td>
      <td>Units</td>
      <td>77.367</td>
      <td>87.274</td>
      <td>...</td>
      <td>106.081</td>
      <td>115.781</td>
      <td>122.077</td>
      <td>135.268</td>
      <td>154.671</td>
      <td>166.406</td>
      <td>176.381</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Philippines</td>
      <td>PH</td>
      <td>PHL</td>
      <td>CO2 Emissions Embodied in Gross Exports</td>
      <td>Millions of metric tons</td>
      <td>ECBTTX</td>
      <td>CO2 Emissions Embodied in Trade; Exports</td>
      <td>Units</td>
      <td>25.255</td>
      <td>23.212</td>
      <td>...</td>
      <td>23.569</td>
      <td>23.604</td>
      <td>26.692</td>
      <td>27.92</td>
      <td>30.655</td>
      <td>36.005</td>
      <td>38.587</td>
      <td>39.484</td>
      <td>32.446</td>
      <td>40.224</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Philippines</td>
      <td>PH</td>
      <td>PHL</td>
      <td>CO2 Emissions Embodied in Gross Exports; balance</td>
      <td>Millions of metric tons</td>
      <td>ECBTT</td>
      <td>CO2 Emissions Embodied in Trade; Trade Balance</td>
      <td>Units</td>
      <td>-16.58</td>
      <td>-21.625</td>
      <td>...</td>
      <td>-22.66</td>
      <td>-23.206</td>
      <td>-23.016</td>
      <td>-27.356</td>
      <td>-35.476</td>
      <td>-35.587</td>
      <td>-40.487</td>
      <td>-49.405</td>
      <td>-39.461</td>
      <td>-53.932</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Philippines</td>
      <td>PH</td>
      <td>PHL</td>
      <td>CO2 Emissions Embodied in Gross Imports</td>
      <td>Millions of metric tons</td>
      <td>ECBTT</td>
      <td>CO2 Emissions Embodied in Trade; Imports</td>
      <td>Units</td>
      <td>41.783</td>
      <td>44.767</td>
      <td>...</td>
      <td>45.863</td>
      <td>46.335</td>
      <td>49.219</td>
      <td>54.679</td>
      <td>65.467</td>
      <td>71.035</td>
      <td>78.565</td>
      <td>88.889</td>
      <td>71.907</td>
      <td>94.156</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 35 columns</p>
</div>




```python
df_carbon_footprints_bro.info()
```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 6 entries, 0 to 5
    Data columns (total 35 columns):
     #   Column          Non-Null Count  Dtype 
    ---  ------          --------------  ----- 
     0   country         6 non-null      object
     1   iso2            6 non-null      object
     2   iso3            6 non-null      object
     3   indicator_used  6 non-null      object
     4   unit            6 non-null      object
     5   cts_code        6 non-null      object
     6   cts_name        6 non-null      object
     7   scale           6 non-null      object
     8   1995            6 non-null      object
     9   1996            6 non-null      object
     10  1997            6 non-null      object
     11  1998            6 non-null      object
     12  1999            6 non-null      object
     13  2000            6 non-null      object
     14  2001            6 non-null      object
     15  2002            6 non-null      object
     16  2003            6 non-null      object
     17  2004            6 non-null      object
     18  2005            6 non-null      object
     19  2006            6 non-null      object
     20  2007            6 non-null      object
     21  2008            6 non-null      object
     22  2009            6 non-null      object
     23  2010            6 non-null      object
     24  2011            6 non-null      object
     25  2012            6 non-null      object
     26  2013            6 non-null      object
     27  2014            6 non-null      object
     28  2015            6 non-null      object
     29  2016            6 non-null      object
     30  2017            6 non-null      object
     31  2018            6 non-null      object
     32  2019            3 non-null      object
     33  2020            3 non-null      object
     34  2021            3 non-null      object
    dtypes: object(35)
    memory usage: 1.8+ KB
    


```python
# Query the 'bronze.ghg_investment_related_bro' table
df_investment_related_bro = pd.read_sql("SELECT * FROM bronze.ghg_investment_related", engine)
df_investment_related_bro.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>country</th>
      <th>iso2</th>
      <th>iso3</th>
      <th>indicator_used</th>
      <th>unit</th>
      <th>cts_code</th>
      <th>cts_name</th>
      <th>sector</th>
      <th>scale</th>
      <th>2005</th>
      <th>...</th>
      <th>2007</th>
      <th>2008</th>
      <th>2009</th>
      <th>2010</th>
      <th>2011</th>
      <th>2012</th>
      <th>2013</th>
      <th>2014</th>
      <th>2015</th>
      <th>2016</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Philippines</td>
      <td>PH</td>
      <td>PHL</td>
      <td>CO2 emissions in exports of domestic controlle...</td>
      <td>Metric tons of CO2</td>
      <td>ECBIXD</td>
      <td>CO2 Emissions in Exports_ Of which: Exports of...</td>
      <td>Accommodation and food services</td>
      <td>Units</td>
      <td>300.822061</td>
      <td>...</td>
      <td>779.4573113</td>
      <td>389.0162687</td>
      <td>344.9649435</td>
      <td>378.0334614</td>
      <td>534.3730055</td>
      <td>927.5316698</td>
      <td>779.8509638</td>
      <td>704.3196699</td>
      <td>813.2396249</td>
      <td>1187.96479,,</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Philippines</td>
      <td>PH</td>
      <td>PHL</td>
      <td>CO2 emissions in exports of domestic controlle...</td>
      <td>Metric tons of CO2</td>
      <td>ECBIXD</td>
      <td>CO2 Emissions in Exports_ Of which: Exports of...</td>
      <td>Agriculture_ forestry and fishing</td>
      <td>Units</td>
      <td>117.8550579</td>
      <td>...</td>
      <td>115.0784213</td>
      <td>106.3545951</td>
      <td>82.23087713</td>
      <td>176.6638541</td>
      <td>201.8047194</td>
      <td>213.9660788</td>
      <td>235.1079027</td>
      <td>216.244946</td>
      <td>136.3344084</td>
      <td>113.4486748,,</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Philippines</td>
      <td>PH</td>
      <td>PHL</td>
      <td>CO2 emissions in exports of domestic controlle...</td>
      <td>Metric tons of CO2</td>
      <td>ECBIXD</td>
      <td>CO2 Emissions in Exports_ Of which: Exports of...</td>
      <td>Arts_ entertainment_ recreation and other serv...</td>
      <td>Units</td>
      <td>92.33922785</td>
      <td>...</td>
      <td>247.2387191</td>
      <td>144.369316</td>
      <td>147.636788</td>
      <td>168.1791097</td>
      <td>233.6003853</td>
      <td>293.5305032</td>
      <td>271.0817259</td>
      <td>274.0178227</td>
      <td>342.7371995</td>
      <td>1978.780861,,</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Philippines</td>
      <td>PH</td>
      <td>PHL</td>
      <td>CO2 emissions in exports of domestic controlle...</td>
      <td>Metric tons of CO2</td>
      <td>ECBIXD</td>
      <td>CO2 Emissions in Exports_ Of which: Exports of...</td>
      <td>Basic metals</td>
      <td>Units</td>
      <td>3.849316616</td>
      <td>...</td>
      <td>31.14188644</td>
      <td>36.37911205</td>
      <td>28.21773481</td>
      <td>44.12235968</td>
      <td>59.76391741</td>
      <td>67.69017702</td>
      <td>41.36996427</td>
      <td>61.70421614</td>
      <td>78.28968306</td>
      <td>77.07163978,,</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Philippines</td>
      <td>PH</td>
      <td>PHL</td>
      <td>CO2 emissions in exports of domestic controlle...</td>
      <td>Metric tons of CO2</td>
      <td>ECBIXD</td>
      <td>CO2 Emissions in Exports_ Of which: Exports of...</td>
      <td>Chemicals and pharmaceutical products</td>
      <td>Units</td>
      <td>34.97580664</td>
      <td>...</td>
      <td>57.92948177</td>
      <td>61.35629543</td>
      <td>61.83910109</td>
      <td>148.6631906</td>
      <td>126.4434094</td>
      <td>221.7047004</td>
      <td>173.896746</td>
      <td>118.5917628</td>
      <td>90.69534842</td>
      <td>78.76884408,,</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 21 columns</p>
</div>




```python
df_investment_related_bro.info()
```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 204 entries, 0 to 203
    Data columns (total 21 columns):
     #   Column          Non-Null Count  Dtype 
    ---  ------          --------------  ----- 
     0   country         204 non-null    object
     1   iso2            204 non-null    object
     2   iso3            204 non-null    object
     3   indicator_used  204 non-null    object
     4   unit            204 non-null    object
     5   cts_code        204 non-null    object
     6   cts_name        204 non-null    object
     7   sector          204 non-null    object
     8   scale           204 non-null    object
     9   2005            204 non-null    object
     10  2006            204 non-null    object
     11  2007            204 non-null    object
     12  2008            204 non-null    object
     13  2009            204 non-null    object
     14  2010            204 non-null    object
     15  2011            204 non-null    object
     16  2012            204 non-null    object
     17  2013            204 non-null    object
     18  2014            204 non-null    object
     19  2015            204 non-null    object
     20  2016            204 non-null    object
    dtypes: object(21)
    memory usage: 33.6+ KB
    

#### Query the table from the silver layer 


```python
# Query the 'silver.ghg_co2_emissions' table
df_co2_emissions = pd.read_sql("SELECT * FROM silver.ghg_co2_emissions", engine)
df_co2_emissions.head()

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>country</th>
      <th>iso2</th>
      <th>iso3</th>
      <th>indicator_used</th>
      <th>unit</th>
      <th>cts_code</th>
      <th>industry</th>
      <th>1995</th>
      <th>1996</th>
      <th>1997</th>
      <th>...</th>
      <th>2009</th>
      <th>2010</th>
      <th>2011</th>
      <th>2012</th>
      <th>2013</th>
      <th>2014</th>
      <th>2015</th>
      <th>2016</th>
      <th>2017</th>
      <th>2018</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Philippines</td>
      <td>PH</td>
      <td>PHL</td>
      <td>CO2 emissions</td>
      <td>Millions of Metric tons of CO2</td>
      <td>ECNC</td>
      <td>Accomodation and food services</td>
      <td>0.141</td>
      <td>0.194</td>
      <td>0.195</td>
      <td>...</td>
      <td>0.324</td>
      <td>0.386</td>
      <td>0.429</td>
      <td>0.446</td>
      <td>0.492</td>
      <td>0.579</td>
      <td>0.540</td>
      <td>0.650</td>
      <td>0.784</td>
      <td>0.852</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Philippines</td>
      <td>PH</td>
      <td>PHL</td>
      <td>CO2 emissions</td>
      <td>Millions of Metric tons of CO2</td>
      <td>ECNC</td>
      <td>Administrative and support services</td>
      <td>0.532</td>
      <td>0.481</td>
      <td>0.476</td>
      <td>...</td>
      <td>0.370</td>
      <td>0.362</td>
      <td>0.313</td>
      <td>0.272</td>
      <td>0.281</td>
      <td>0.340</td>
      <td>0.333</td>
      <td>0.384</td>
      <td>0.481</td>
      <td>0.539</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Philippines</td>
      <td>PH</td>
      <td>PHL</td>
      <td>CO2 emissions</td>
      <td>Millions of Metric tons of CO2</td>
      <td>ECNC</td>
      <td>Agriculture, hunting, forestry</td>
      <td>0.506</td>
      <td>0.582</td>
      <td>0.620</td>
      <td>...</td>
      <td>0.292</td>
      <td>0.306</td>
      <td>0.314</td>
      <td>0.319</td>
      <td>0.343</td>
      <td>0.335</td>
      <td>0.353</td>
      <td>0.394</td>
      <td>0.471</td>
      <td>0.449</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Philippines</td>
      <td>PH</td>
      <td>PHL</td>
      <td>CO2 emissions</td>
      <td>Millions of Metric tons of CO2</td>
      <td>ECNC</td>
      <td>Air transport</td>
      <td>2.241</td>
      <td>2.596</td>
      <td>2.991</td>
      <td>...</td>
      <td>3.740</td>
      <td>3.986</td>
      <td>4.325</td>
      <td>4.538</td>
      <td>4.840</td>
      <td>5.684</td>
      <td>5.692</td>
      <td>6.552</td>
      <td>6.825</td>
      <td>7.199</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Philippines</td>
      <td>PH</td>
      <td>PHL</td>
      <td>CO2 emissions</td>
      <td>Millions of Metric tons of CO2</td>
      <td>ECNC</td>
      <td>Arts, entertainment and recreation</td>
      <td>0.087</td>
      <td>0.109</td>
      <td>0.137</td>
      <td>...</td>
      <td>0.132</td>
      <td>0.143</td>
      <td>0.146</td>
      <td>0.142</td>
      <td>0.158</td>
      <td>0.188</td>
      <td>0.174</td>
      <td>0.211</td>
      <td>0.256</td>
      <td>0.277</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 31 columns</p>
</div>




```python
df_co2_emissions.info()
```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 132 entries, 0 to 131
    Data columns (total 31 columns):
     #   Column          Non-Null Count  Dtype  
    ---  ------          --------------  -----  
     0   country         132 non-null    object 
     1   iso2            132 non-null    object 
     2   iso3            132 non-null    object 
     3   indicator_used  132 non-null    object 
     4   unit            132 non-null    object 
     5   cts_code        132 non-null    object 
     6   industry        132 non-null    object 
     7   1995            132 non-null    float64
     8   1996            132 non-null    float64
     9   1997            132 non-null    float64
     10  1998            132 non-null    float64
     11  1999            132 non-null    float64
     12  2000            132 non-null    float64
     13  2001            132 non-null    float64
     14  2002            132 non-null    float64
     15  2003            132 non-null    float64
     16  2004            132 non-null    float64
     17  2005            132 non-null    float64
     18  2006            132 non-null    float64
     19  2007            132 non-null    float64
     20  2008            132 non-null    float64
     21  2009            132 non-null    float64
     22  2010            132 non-null    float64
     23  2011            132 non-null    float64
     24  2012            132 non-null    float64
     25  2013            132 non-null    float64
     26  2014            132 non-null    float64
     27  2015            132 non-null    float64
     28  2016            132 non-null    float64
     29  2017            132 non-null    float64
     30  2018            132 non-null    float64
    dtypes: float64(24), object(7)
    memory usage: 32.1+ KB
    


```python
# Query the 'silver.ghg_carbon_footprints' table
df_carbon_footprints = pd.read_sql("SELECT * FROM silver.ghg_carbon_footprints", engine)
df_carbon_footprints.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>country</th>
      <th>iso2</th>
      <th>iso3</th>
      <th>indicator_used</th>
      <th>unit</th>
      <th>cts_code</th>
      <th>1995</th>
      <th>1996</th>
      <th>1997</th>
      <th>1998</th>
      <th>...</th>
      <th>2012</th>
      <th>2013</th>
      <th>2014</th>
      <th>2015</th>
      <th>2016</th>
      <th>2017</th>
      <th>2018</th>
      <th>2019</th>
      <th>2020</th>
      <th>2021</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Philippines</td>
      <td>PH</td>
      <td>PHL</td>
      <td>CO2 Emissions Embodied in Final Demand; balance</td>
      <td>Millions of metric tons</td>
      <td>ECBTDB</td>
      <td>-16.579</td>
      <td>-21.626</td>
      <td>-26.710</td>
      <td>-18.727</td>
      <td>...</td>
      <td>-22.660</td>
      <td>-23.206</td>
      <td>-23.016</td>
      <td>-27.356</td>
      <td>-35.476</td>
      <td>-35.587</td>
      <td>-40.487</td>
      <td>0.000</td>
      <td>0.000</td>
      <td>0.000</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Philippines</td>
      <td>PH</td>
      <td>PHL</td>
      <td>CO2 Emissions Embodied in Final Domestic Demand</td>
      <td>Millions of metric tons</td>
      <td>ECBTDD</td>
      <td>77.367</td>
      <td>87.274</td>
      <td>99.070</td>
      <td>90.788</td>
      <td>...</td>
      <td>106.081</td>
      <td>115.781</td>
      <td>122.077</td>
      <td>135.268</td>
      <td>154.671</td>
      <td>166.406</td>
      <td>176.381</td>
      <td>0.000</td>
      <td>0.000</td>
      <td>0.000</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Philippines</td>
      <td>PH</td>
      <td>PHL</td>
      <td>CO2 Emissions Embodied in Gross Exports</td>
      <td>Millions of metric tons</td>
      <td>ECBTTX</td>
      <td>25.255</td>
      <td>23.212</td>
      <td>26.831</td>
      <td>24.716</td>
      <td>...</td>
      <td>23.569</td>
      <td>23.604</td>
      <td>26.692</td>
      <td>27.920</td>
      <td>30.655</td>
      <td>36.005</td>
      <td>38.587</td>
      <td>39.484</td>
      <td>32.446</td>
      <td>40.224</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Philippines</td>
      <td>PH</td>
      <td>PHL</td>
      <td>CO2 Emissions Embodied in Gross Exports; balance</td>
      <td>Millions of metric tons</td>
      <td>ECBTT</td>
      <td>-16.580</td>
      <td>-21.625</td>
      <td>-26.709</td>
      <td>-18.725</td>
      <td>...</td>
      <td>-22.660</td>
      <td>-23.206</td>
      <td>-23.016</td>
      <td>-27.356</td>
      <td>-35.476</td>
      <td>-35.587</td>
      <td>-40.487</td>
      <td>-49.405</td>
      <td>-39.461</td>
      <td>-53.932</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Philippines</td>
      <td>PH</td>
      <td>PHL</td>
      <td>CO2 Emissions Embodied in Gross Imports</td>
      <td>Millions of metric tons</td>
      <td>ECBTT</td>
      <td>41.783</td>
      <td>44.767</td>
      <td>53.479</td>
      <td>43.424</td>
      <td>...</td>
      <td>45.863</td>
      <td>46.335</td>
      <td>49.219</td>
      <td>54.679</td>
      <td>65.467</td>
      <td>71.035</td>
      <td>78.565</td>
      <td>88.889</td>
      <td>71.907</td>
      <td>94.156</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 33 columns</p>
</div>




```python
df_carbon_footprints.info()
```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 6 entries, 0 to 5
    Data columns (total 33 columns):
     #   Column          Non-Null Count  Dtype  
    ---  ------          --------------  -----  
     0   country         6 non-null      object 
     1   iso2            6 non-null      object 
     2   iso3            6 non-null      object 
     3   indicator_used  6 non-null      object 
     4   unit            6 non-null      object 
     5   cts_code        6 non-null      object 
     6   1995            6 non-null      float64
     7   1996            6 non-null      float64
     8   1997            6 non-null      float64
     9   1998            6 non-null      float64
     10  1999            6 non-null      float64
     11  2000            6 non-null      float64
     12  2001            6 non-null      float64
     13  2002            6 non-null      float64
     14  2003            6 non-null      float64
     15  2004            6 non-null      float64
     16  2005            6 non-null      float64
     17  2006            6 non-null      float64
     18  2007            6 non-null      float64
     19  2008            6 non-null      float64
     20  2009            6 non-null      float64
     21  2010            6 non-null      float64
     22  2011            6 non-null      float64
     23  2012            6 non-null      float64
     24  2013            6 non-null      float64
     25  2014            6 non-null      float64
     26  2015            6 non-null      float64
     27  2016            6 non-null      float64
     28  2017            6 non-null      float64
     29  2018            6 non-null      float64
     30  2019            6 non-null      float64
     31  2020            6 non-null      float64
     32  2021            6 non-null      float64
    dtypes: float64(27), object(6)
    memory usage: 1.7+ KB
    


```python
# Query the 'silver.ghg_investment_related' table
df_investment_related = pd.read_sql("SELECT * FROM silver.ghg_investment_related", engine)
df_investment_related.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>country</th>
      <th>iso2</th>
      <th>iso3</th>
      <th>indicator_used</th>
      <th>unit</th>
      <th>cts_code</th>
      <th>sector</th>
      <th>2005</th>
      <th>2006</th>
      <th>2007</th>
      <th>2008</th>
      <th>2009</th>
      <th>2010</th>
      <th>2011</th>
      <th>2012</th>
      <th>2013</th>
      <th>2014</th>
      <th>2015</th>
      <th>2016</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Philippines</td>
      <td>PH</td>
      <td>PHL</td>
      <td>CO2 emissions in exports of domestic controlle...</td>
      <td>Metric tons of CO2</td>
      <td>ECBIXD</td>
      <td>Accommodation and food services</td>
      <td>300.822061</td>
      <td>745.219482</td>
      <td>779.457311</td>
      <td>389.016269</td>
      <td>344.964944</td>
      <td>378.033461</td>
      <td>534.373005</td>
      <td>927.531670</td>
      <td>779.850964</td>
      <td>704.319670</td>
      <td>813.239625</td>
      <td>1187.964790</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Philippines</td>
      <td>PH</td>
      <td>PHL</td>
      <td>CO2 emissions in exports of domestic controlle...</td>
      <td>Metric tons of CO2</td>
      <td>ECBIXD</td>
      <td>Agriculture, forestry and fishing</td>
      <td>117.855058</td>
      <td>187.789333</td>
      <td>115.078421</td>
      <td>106.354595</td>
      <td>82.230877</td>
      <td>176.663854</td>
      <td>201.804719</td>
      <td>213.966079</td>
      <td>235.107903</td>
      <td>216.244946</td>
      <td>136.334408</td>
      <td>113.448675</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Philippines</td>
      <td>PH</td>
      <td>PHL</td>
      <td>CO2 emissions in exports of domestic controlle...</td>
      <td>Metric tons of CO2</td>
      <td>ECBIXD</td>
      <td>Arts, entertainment, recreation and other serv...</td>
      <td>92.339228</td>
      <td>210.362293</td>
      <td>247.238719</td>
      <td>144.369316</td>
      <td>147.636788</td>
      <td>168.179110</td>
      <td>233.600385</td>
      <td>293.530503</td>
      <td>271.081726</td>
      <td>274.017823</td>
      <td>342.737199</td>
      <td>1978.780861</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Philippines</td>
      <td>PH</td>
      <td>PHL</td>
      <td>CO2 emissions in exports of domestic controlle...</td>
      <td>Metric tons of CO2</td>
      <td>ECBIXD</td>
      <td>Basic metals</td>
      <td>3.849317</td>
      <td>38.794226</td>
      <td>31.141886</td>
      <td>36.379112</td>
      <td>28.217735</td>
      <td>44.122360</td>
      <td>59.763917</td>
      <td>67.690177</td>
      <td>41.369964</td>
      <td>61.704216</td>
      <td>78.289683</td>
      <td>77.071640</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Philippines</td>
      <td>PH</td>
      <td>PHL</td>
      <td>CO2 emissions in exports of domestic controlle...</td>
      <td>Metric tons of CO2</td>
      <td>ECBIXD</td>
      <td>Chemicals and pharmaceutical products</td>
      <td>34.975807</td>
      <td>69.452667</td>
      <td>57.929482</td>
      <td>61.356295</td>
      <td>61.839101</td>
      <td>148.663191</td>
      <td>126.443409</td>
      <td>221.704700</td>
      <td>173.896746</td>
      <td>118.591763</td>
      <td>90.695348</td>
      <td>78.768844</td>
    </tr>
  </tbody>
</table>
</div>




```python
df_investment_related.info()
```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 190 entries, 0 to 189
    Data columns (total 19 columns):
     #   Column          Non-Null Count  Dtype  
    ---  ------          --------------  -----  
     0   country         190 non-null    object 
     1   iso2            190 non-null    object 
     2   iso3            190 non-null    object 
     3   indicator_used  190 non-null    object 
     4   unit            190 non-null    object 
     5   cts_code        190 non-null    object 
     6   sector          190 non-null    object 
     7   2005            190 non-null    float64
     8   2006            190 non-null    float64
     9   2007            190 non-null    float64
     10  2008            190 non-null    float64
     11  2009            190 non-null    float64
     12  2010            190 non-null    float64
     13  2011            190 non-null    float64
     14  2012            190 non-null    float64
     15  2013            190 non-null    float64
     16  2014            190 non-null    float64
     17  2015            190 non-null    float64
     18  2016            190 non-null    float64
    dtypes: float64(12), object(7)
    memory usage: 28.3+ KB
    

#### Export the table to the local file in '.csv' format


```python
# Export 'silver.ghg_co2_emissions' table
df_co2_emissions.to_csv(r"C:\Users\ojoar\Desktop\PH_All_Indicators\ghg\clean_table_exported_from_sql\co2_emissions.csv", index = False)

# Export 'silver.ghg_carbon_footprints' table
df_carbon_footprints.to_csv(r"C:\Users\ojoar\Desktop\PH_All_Indicators\ghg\clean_table_exported_from_sql\carbon_footprints.csv", index = False)

# Export 'silver.ghg_investment_related' table
df_investment_related.to_csv(r"C:\Users\ojoar\Desktop\PH_All_Indicators\ghg\clean_table_exported_from_sql\investment_related.csv", index = False)


```


    ---------------------------------------------------------------------------

    PermissionError                           Traceback (most recent call last)

    Cell In[16], line 8
          5 df_carbon_footprints.to_csv(r"C:\Users\ojoar\Desktop\PH_All_Indicators\ghg\clean_table_exported_from_sql\carbon_footprints.csv", index = False)
          7 # Export 'silver.ghg_investment_related' table
    ----> 8 df_investment_related.to_csv(r"C:\Users\ojoar\Desktop\PH_All_Indicators\ghg\clean_table_exported_from_sql\investment_related.csv", index = False)
    

    File C:\ProgramData\anaconda3\Lib\site-packages\pandas\util\_decorators.py:333, in deprecate_nonkeyword_arguments.<locals>.decorate.<locals>.wrapper(*args, **kwargs)
        327 if len(args) > num_allow_args:
        328     warnings.warn(
        329         msg.format(arguments=_format_argument_list(allow_args)),
        330         FutureWarning,
        331         stacklevel=find_stack_level(),
        332     )
    --> 333 return func(*args, **kwargs)
    

    File C:\ProgramData\anaconda3\Lib\site-packages\pandas\core\generic.py:3967, in NDFrame.to_csv(self, path_or_buf, sep, na_rep, float_format, columns, header, index, index_label, mode, encoding, compression, quoting, quotechar, lineterminator, chunksize, date_format, doublequote, escapechar, decimal, errors, storage_options)
       3956 df = self if isinstance(self, ABCDataFrame) else self.to_frame()
       3958 formatter = DataFrameFormatter(
       3959     frame=df,
       3960     header=header,
       (...)
       3964     decimal=decimal,
       3965 )
    -> 3967 return DataFrameRenderer(formatter).to_csv(
       3968     path_or_buf,
       3969     lineterminator=lineterminator,
       3970     sep=sep,
       3971     encoding=encoding,
       3972     errors=errors,
       3973     compression=compression,
       3974     quoting=quoting,
       3975     columns=columns,
       3976     index_label=index_label,
       3977     mode=mode,
       3978     chunksize=chunksize,
       3979     quotechar=quotechar,
       3980     date_format=date_format,
       3981     doublequote=doublequote,
       3982     escapechar=escapechar,
       3983     storage_options=storage_options,
       3984 )
    

    File C:\ProgramData\anaconda3\Lib\site-packages\pandas\io\formats\format.py:1014, in DataFrameRenderer.to_csv(self, path_or_buf, encoding, sep, columns, index_label, mode, compression, quoting, quotechar, lineterminator, chunksize, date_format, doublequote, escapechar, errors, storage_options)
        993     created_buffer = False
        995 csv_formatter = CSVFormatter(
        996     path_or_buf=path_or_buf,
        997     lineterminator=lineterminator,
       (...)
       1012     formatter=self.fmt,
       1013 )
    -> 1014 csv_formatter.save()
       1016 if created_buffer:
       1017     assert isinstance(path_or_buf, StringIO)
    

    File C:\ProgramData\anaconda3\Lib\site-packages\pandas\io\formats\csvs.py:251, in CSVFormatter.save(self)
        247 """
        248 Create the writer & save.
        249 """
        250 # apply compression and byte/text conversion
    --> 251 with get_handle(
        252     self.filepath_or_buffer,
        253     self.mode,
        254     encoding=self.encoding,
        255     errors=self.errors,
        256     compression=self.compression,
        257     storage_options=self.storage_options,
        258 ) as handles:
        259     # Note: self.encoding is irrelevant here
        260     self.writer = csvlib.writer(
        261         handles.handle,
        262         lineterminator=self.lineterminator,
       (...)
        267         quotechar=self.quotechar,
        268     )
        270     self._save()
    

    File C:\ProgramData\anaconda3\Lib\site-packages\pandas\io\common.py:873, in get_handle(path_or_buf, mode, encoding, compression, memory_map, is_text, errors, storage_options)
        868 elif isinstance(handle, str):
        869     # Check whether the filename is to be opened in binary mode.
        870     # Binary mode does not support 'encoding' and 'newline'.
        871     if ioargs.encoding and "b" not in ioargs.mode:
        872         # Encoding
    --> 873         handle = open(
        874             handle,
        875             ioargs.mode,
        876             encoding=ioargs.encoding,
        877             errors=errors,
        878             newline="",
        879         )
        880     else:
        881         # Binary mode
        882         handle = open(handle, ioargs.mode)
    

    PermissionError: [Errno 13] Permission denied: 'C:\\Users\\ojoar\\Desktop\\PH_All_Indicators\\ghg\\clean_table_exported_from_sql\\investment_related.csv'



```python

```
