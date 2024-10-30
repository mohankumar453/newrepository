# Databricks notebook source
# MAGIC   %run ./helpers/cube_notebook

# COMMAND ----------

#class and functions Cube is defined in /helpers/cube_notebook
c1 = Cube(3)
c1.get_volume()

# COMMAND ----------

from helpers.cube_notebook import Cube
#above command will fail as you can not import the functions from notebook. instead you can create .py file and import that package

# COMMAND ----------

#imports python package from helpers folder
from helpers.cube import Cube_PY

# COMMAND ----------

#Cube_PY is from  helpers/cube.py file
c2 = Cube_PY(3)
c2.get_volume()

# COMMAND ----------

#%sh magic allows you run shell commands in the notebook
%sh pwd

# COMMAND ----------

# MAGIC %sh ls ./helpers

# COMMAND ----------

#sys.path is a list of directories where Python interpreter searches for modules. To explore the list of directories in the path variable, you need first import the sys module.
import sys

for path in sys.path:
    print(path)

# COMMAND ----------

#We can use the method Sys.path.append to add our modules directory to the path variable.
#you will see /Workspace/Repos/mohanalagesan12345@gmail.com/Databricks-DataEngineer-Professional/Course+Materials/7 - Testing and Deployment/7.1 - Relative Imports/modules is showing up in the output below
import os
sys.path.append(os.path.abspath('../modules'))

# COMMAND ----------

for path in sys.path:
    print(path)

# COMMAND ----------

#import Cube from shapes module in the modules directory
#it fails since init.py file not present in the modelues folder
from shapes.cube import Cube as CubeShape

# COMMAND ----------

c3 = CubeShape(3)
c3.get_volume()

# COMMAND ----------

#installing python wheel using pip command. using sh command pip install will install the libraries only to driver nodes. %pip magic command will install to all the nodes in the cluster
%pip install ../wheels/shapes-1.0.0-py3-none-any.whl
#once above runs notice  that the python interpreter has been restarted. This clears all the variables declared in this notebook. This is why you should place all the PIP commands at the beginning of your notebook

# COMMAND ----------

from shapes_wheel.cube import Cube as Cube_WHL

# COMMAND ----------

c4 = Cube_WHL(3)
c4.get_volume()

# COMMAND ----------

#%sh pip install ../wheels/shapes-1.0.0-py3-none-any.whl
