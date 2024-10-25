# stockspy
A python script retrieving stock data from twelvedata, doing some calculation and storing it in msql.

## Installation 

Navigate to the desired installation directory
```
cd /home/user/python
```
Clone the repository from GitHub
```
git clone https://github.com/drhdev/stockspy.git
```
Navigate to the cloned repository
```
cd stockspy
```
Create a virtual environment named 'venv' inside the stockspy directory
```
python3 -m venv venv
```
Activate the virtual environment
```
source venv/bin/activate
```
Install required packages from requirements.txt (without version numbers)
```
pip install -r requirements.txt
```
Ensure the .env file is in place with the correct configurations, then run the script
```
python stockspy.py
```
Optional: To deactivate the virtual environment after running
```
deactivate
```
