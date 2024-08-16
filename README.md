# Ninja 7 Capstone Project for MGMT 590 - AI-Assisted Big Data Analytics in the Cloud
This repository contains all the code and dataset for our Ninja 7 Group Capstone Project.

**Project Members**: Yong Li, Clayton Jones, Ed Gomez, Michael Palmer, Rhonda Spence

**Summary**: Our project leverages Google Cloud Platform (GCP) technologies to build a portfolio that filters for profitable stocks. We will use a combination of DCF analysis, dynamic financial indicators, SMA, and the Sharpe Ratio to select the most promising stocks. Additionally, we will apply an HMM model to predict stock prices and identify trading opportunities. 

**Installation**

**1.) Download the necessary Python libraries**

pip install tushare

pip install pymysql

pip install xlwt

pip install hmmlearn

pip install akshare

**2.) Install MySQL**

https://dev.mysql.com/downloads/installer/

Select mysql-installer-community-8.0.39.0.msi

**3.) Register on Tushare**

https://tushare.pro/register

**4.) Download the Python files in this repository**

**5.) Update MySQL User and Passwords in Python files**

For dcf.py update lines: 1071, 1089, 1105, 1115

For financialReportIndicators.py update lines: 14, 103

For generatePortfolio.py update line: 22

**6.) All compressed files labeled '...function_source' need to be uploaded to Google Cloud Function**




