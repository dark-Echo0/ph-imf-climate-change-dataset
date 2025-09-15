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






```python

```

### Install the required package


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
    

### Import Libraries


```python
from sqlalchemy import create_engine
import pandas as pd

server = 'LAPTOP-LJ0109LP\SQLEXPRESS'      # e.g., 'localhost\SQLEXPRESS'
database = 'PHClimateChangeDataset'

engine = create_engine(
    f"mssql+pyodbc://@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
)

```

### Query the table from the Database 'PHClimateChnageDataset' created using SQL SERVER


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
    

### Export the table to the local file in '.csv' format


```python
# Export 'silver.ghg_co2_emissions' table
df_co2_emissions.to_csv(r"C:\Users\ojoar\Desktop\PH_All_Indicators\ghg\clean_table_exported_from_sql\co2_emissions.csv", index = False)

# Export 'silver.ghg_carbon_footprints' table
df_carbon_footprints.to_csv(r"C:\Users\ojoar\Desktop\PH_All_Indicators\ghg\clean_table_exported_from_sql\carbon_footprints.csv", index = False)

# Export 'silver.ghg_investment_related' table
df_investment_related.to_csv(r"C:\Users\ojoar\Desktop\PH_All_Indicators\ghg\clean_table_exported_from_sql\investment_related.csv", index = False)


```


```python

```
