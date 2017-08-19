# Team6_Final_Project_World_bank_data

# Instruction to Run the code

- Clone repository from github in to your homedirectory
- To get the images in the local execute: 
   - docker pull prashantvksingh/ads_final:team6finalproject
- Execute  docker run -e AWS_ACCESS_KEY="AKIAJL2ZPNORFYQ74FVQ" -e AWS_SECRET_KEY="LijYsR9NdNIJd/j2XBHPBAAKttphXOO6w1/a+Wcn" prashantvksingh/ads_final:team6finalproject /usr/src/ADS_Final/run.sh
  - Replace AWS_ACCESS_KEY, AWS_SECRET_KEY with your ownn key
  - Execute docker ps -l (to get the ID of last run container)
  - Execute docker commit 'ID received from above code' prashantvksingh/ads_final:team6finalproject
  
- Link of demo : https://youtu.be/ISkz9hkdntg

- WebApp URL : http://54.165.65.21/
