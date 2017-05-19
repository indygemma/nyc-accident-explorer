"""
Copyright (c) 2017 Conrad Indiono

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.  See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program (see file COPYING). If not, see <http://www.gnu.org/licenses/>.
"""

import pandas as pd

original_df = pd.read_csv("NYPD_Motor_Vehicle_Collisions.csv")

for filename in ["clusters_40m.csv", "clusters_30m.csv", "clusters_25m.csv", "clusters_10m.csv"]:
    df = pd.read_csv(filename)
    original_df = original_df.merge(df.drop("latitude", 1).drop("longitude", 1), left_on="UNIQUE KEY", right_on="uniquekey")
    original_df = original_df.drop("uniquekey", 1)

original_df.to_csv("combined.csv", index=False)
