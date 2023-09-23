# coding=utf-8
# import necessary modules
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, MultiPoint
#from shapely import GeometryCollection, LineString, MultiPoint, Polygon
from shapely.ops import triangulate
import matplotlib.pyplot as plt
import os
import math
FILE_NAME = "NYC_LOCID.pkl"

# calculer la BBox de chaque borough ?
# geo_df2 = data_wgs84.dissolve(by='borough')['geometry'].apply(lambda x: x.envelope)
# pas de gain notable
STEP = .001

enlarge = 0

def t(x):
    return round(x/STEP)

# Charge le terrain pretraite, ou pretraite le terrain
if os.path.isfile(FILE_NAME):
    data_df =  pd.read_pickle(FILE_NAME)
    print(data_df.describe())
else:  # generate file takes 35 s
# Read file using gpd.read_file()
    data = gpd.read_file("taxi_zones.zip")
# return to long / lat 
    data_wgs84 = data.to_crs({'init': 'epsg:4326'})

    data_wgs84["geometry"] = data_wgs84.geometry.simplify(tolerance=0.001,preserve_topology=True)
# we can check extrem values
#print(data_wgs84.bounds.describe())

# Utilisez la methode total_bounds pour obtenir la bbox
    bbox = data_wgs84.total_bounds

    min_x, min_y, max_x, max_y = bbox
    #print(f"min_x: {min_x}, max_x: {max_x}, min_y: {min_y}, max_y: {max_y}")

    colonnes = ['LocationID', 'zone', 'borough','geometry']
    df = pd.DataFrame(columns=colonnes)

    # remove Multi polygon
    data_wgs84 = data_wgs84.explode()


    for i in range(len(data_wgs84)):

        data =  data_wgs84.iloc[i]
        shape = data['geometry']


#   print(type(shape),dir(shape))
#   print(type(shape['geometry']))
#   print("\n",dir(shape))  

        print(i,data['zone'],data['LocationID'])

        locId = data['LocationID']

#        if locId !=   13 : # 13 #153:
#           continue

        zone = data['zone']
        borough = data['borough']

        #triangulate and remove false values (if shape is not convex)
        triangle_list = [triangle for triangle in triangulate(shape) if triangle.centroid.within(shape)]

        data = { colonnes[0]: [locId] * len(triangle_list),
                 colonnes[1]: [zone] * len(triangle_list),
                 colonnes[2]: [borough] * len(triangle_list),
                 colonnes[3]: triangle_list }

        df_new_rows = pd.DataFrame(data)

        df = pd.concat([df, df_new_rows])

    df.to_pickle(FILE_NAME)
    data_df = df

#print(data_df)

gdf = gpd.GeoDataFrame(data_df)
gdf.plot(column="LocationID") # column='geometry') #.scatter(x="longitude", y="latitude", c="LocationID", colormap='viridis')
plt.savefig("current.png", bbox_inches='tight', pad_inches=0, dpi=300)


#long_r = [STEP*t(min_x) + i * STEP for i in range(round(1+(max_x-min_x)/STEP))]
#lat_r =  [STEP*t(min_y) + i * STEP for i in range(round(1+(max_y-min_y)/STEP))]

#print (len(long_r),len(lat_r))

colonnes = ['longitude', 'latitude', 'LocationID','zone','borough']
df = pd.DataFrame(columns=colonnes)

#dfs_to_concat = []

print("len:",len(data_df))

#ii = 0

for data in data_df.itertuples(): #  i in range(len(data_df)):
#      ii=ii+1
#      print(ii)


      #data =  data_df.iloc[i]
      shape = data.geometry #['geometry']


      locId = data.LocationID # ['LocationID']
      zone = data.zone#['zone']
      borough = data.borough # ['borough']  

      A = shape.exterior.coords[0]
      B = shape.exterior.coords[1]
      C = shape.exterior.coords[2]

      # find top left, bottom left and last point (right)   
      # Etape 1 : Trouver le point TL
      if A[1] > B[1]:
          TL = A
      elif A[1] < B[1]:
          TL = B
      else:
          if A[0] < B[0]:
              TL = A
          else:
              TL = B

      # Comparer TL avec C pour determiner si C devrait etre TL
      if C[1] > TL[1]:
          TL = C
      elif C[1] == TL[1] and C[0] < TL[0]:
          TL = C

      # Etape 2 : Trouver le point LT parmi les deux restants
      if TL == A:
          if B[0] < C[0]:
              LB = B
          elif B[0] > C[0]:
              LB = C
          else:
              if B[1] < C[1]:
                  LB = C
              else:
                  LB = B
      elif TL == B:


          if A[0] < C[0]:
              LB = A
          elif A[0] > C[0]:
              LB = C
          else:
              if A[1] < C[1]:
                  LB = C
              else:
                  LB = A
      else:   # TL ) C
          if A[0] < B[0]:
              LB = A
          elif A[0] > B[0]:
              LB = B
          else:
              if A[1] < B[1]:
                  LB = B
              else:
                  LB = A

      # Étape 3 : Le dernier point restant devient R pas necessairement a droite ...
      if TL == A:
          if LB == B:
              R = C
          else:
              R = B
      elif TL == B :
          if LB == A:
              R = C
          else:
              R = A
      else:
          if LB == A:
              R = B
          else:
              R = A

      L = LB

      if False and TL[1] == L[1]:
         print("witch")
         L,R = R,L

      min_x = min(TL[0],L[0],R[0])
      max_x = max(TL[0],L[0],R[0])
      min_y = min(TL[1],L[1],R[1])
      max_y = max(TL[1],L[1],R[1])

      #print(TL,L,R)
      # fill triangle in 2 steps : below and above the intermediate latitude (neither max nor min)

      if R[1]  == max_y or L[1] == max_y : # ici on n'aura pas de deuxieme partie a remplir (pas de latitude iitnermediaire)
         lat_medium = min_y
      else:
         lat_medium = max(L[1],R[1])

      #lat_r =  [STEP*t(L[1]) + i*STEP  for i in range(round((TL[1]-L[1])/STEP))]
      lat_r =  [lat_medium + i*STEP  for i in range(enlarge+ math.floor((TL[1]-lat_medium)/STEP))]
      #print("lat:",lat_r[0],len(lat_r))

      b = -(TL[0] - L[0]) / (TL[1] - L[1])
      c = -L[0] - b * L[1]

      for latitude in lat_r:
      # droite de gauche x + b * y + c = 0

         long_c = -b*latitude-c

         if TL[1] == R[1] :
            bp = -(L[0] - R[0]) / (L[1] - R[1])# +.00000000001)
            cp = -R[0] - bp * R[1]
            long_c_2 = -bp*latitude-cp

         else:
            bp = -(TL[0] - R[0]) / (TL[1] - R[1])# +.00000000001)
            cp = -R[0] - bp * R[1]
            long_c_2 = -bp*latitude-cp


         if  long_c > long_c_2:
            long_c,long_c_2=long_c_2,long_c

         if  long_c_2 > max_x or long_c_2 < min_x:
               ##print("****",latitude,lat_r[0],lat_r[-1],long_c_2,min_x,max_x,"!",bp,cp)
               long_c_2 = min(long_c_2,max_x)
               lonc_c_2 = max(long_c_2,min_x)

         long_r = [t(long_c+i*STEP)  for i in range(enlarge+math.floor((long_c_2-long_c)/STEP))]

         num_values = len(long_r)
         data = { 'longitude': long_r ,
                   'latitude': [t(latitude)] * num_values,
                   'LocationID': [locId] * num_values,
                   'zone': [zone] * num_values,
                   'borough': [borough] * num_values }

         df_new_rows = pd.DataFrame(data)

         #print(df_new_rows)

         df = pd.concat([df, df_new_rows], ignore_index=True)

      # droite de gauche x + b * y + c = 0   change

      if True and lat_medium > min_y:
         lat_r = [min_y + i*STEP  for i in range(enlarge+math.floor((lat_medium-min_y)/STEP))]

         if R[1] == min_y:
           b = -(R[0] - TL[0]) / (R[1] - TL[1])
           c = -TL[0] - b * TL[1]

           bp = -(L[0] - R[0]) / (L[1] - R[1])
           cp = -R[0] - bp * R[1]
         else:

           b = -(R[0] - L[0]) / (R[1] - L[1])
           c = -L[0] - b * L[1]

           bp = -(TL[0] - L[0]) / (TL[1] - L[1])
           cp = -L[0] - bp * L[1]
      else:
         lat_r = []


      for latitude in lat_r:

         long_min_c = -b*latitude-c
         long_max_c = -bp*latitude-cp


         if long_min_c > long_max_c:
            long_min_c,long_max_c=long_max_c,long_min_c

         if  long_min_c > max_x or long_min_c < min_x:
               #print(TL,L,R,lat_medium)

               #print("===",latitude,lat_r[0],lat_r[-1],long_min_c,min_x,max_x)
               #print(f"min_x: {min_x}, max_x: {max_x}, min_y: {min_y}, max_y: {max_y}")
               long_min_c = min(long_min_c,max_x)
               long_min_c = max(long_min_c,min_x)

         if  long_max_c > max_x or long_max_c < min_x:
               #print(TL,L,R,lat_medium)
               #print(f"min_x: {min_x}, max_x: {max_x}, min_y: {min_y}, max_y: {max_y}")
               #print("###",latitude,lat_r[0],lat_r[-1],len(lat_r),long_max_c,min_x,max_x,bp,cp)
               long_max_c = min(long_max_c,max_x)
               long_max_c = max(long_max_c,min_x)



         long_r = [t(long_min_c + i*STEP)   for i in range(enlarge+math.floor((long_max_c-long_min_c)/STEP))]
         num_values = len(long_r)
         data = { 'longitude': long_r,
                   'latitude': [t(latitude)] * num_values,
                   'LocationID': [locId] * num_values,
                   'zone': [zone] * num_values,
                   'borough': [borough] * num_values }

         df_new_rows = pd.DataFrame(data)
         #dfs_to_concat.append(df_new_rows)

         #dfs_to_concat = []
         #df = pd.concat([dfs_to_concat, df_new_rows])
         df = pd.concat([df, df_new_rows],ignore_index=True)


# Concaténer tous les DataFrames à la fin de la boucle
#df = pd.concat(dfs_to_concat, ignore_index=True)

df = df.sort_values(['longitude', 'latitude'], ascending = [True, True])
df.drop_duplicates(keep = 'first', inplace=True)

# todo verifier en image
df.plot.scatter(x="longitude", y="latitude", c="LocationID", colormap='viridis')
#) # , s=50);


# Utilisez Matplotlib pour afficher l'image
#plt.imshow(df, cmap='gray')  # cmap='gray' pour une image en niveaux de gris
#plt.axis('off')  # Masque les axes

# Enregistrez l'image au format souhaité (par exemple, PNG)
plt.savefig("image.png", bbox_inches='tight', pad_inches=0, dpi=300)

#print(df.describe())         


#df.drop_duplicates(keep = 'first', inplace=True)

# print(df.describe())

# install pyarrow or fastparquet to use :
df.to_parquet("taxis_zone.parquet", index=False)  # Vous pouvez inclure l'index si nécessaire

