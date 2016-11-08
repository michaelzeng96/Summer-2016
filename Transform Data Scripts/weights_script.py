import pandas as pd
import os

user_input = raw_input("Enter full path including filename (i.e. 'C:\Downloads\Wingz Driver Ride Distribution Preference.csv'):   ")

filename = os.path.abspath(user_input)
df = pd.read_csv(filename)

new_df = pd.DataFrame(columns=['email_utilisateur','id_zone','id_utilisateur','weight'])


