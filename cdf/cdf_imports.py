# Databricks notebook source
import pandas as pd
import json
import numpy as np 
from dateutil import parser
import os
import matplotlib.pyplot as plt
import scipy.stats as stats
from scipy.stats import gamma
from scipy import stats
import plotly.graph_objs as go
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from PIL import Image
from calendar import monthrange
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file_creation_information import FileCreationInformation
import plotly.graph_objects as go
from IPython.display import HTML, display
import urllib.parse

# COMMAND ----------

