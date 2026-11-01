FROM python:3.9 

WORKDIR /AshaFundingAnalysis

COPY requirements.txt . 

RUN pip install -r requirements.txt

#COPY . .

#build cmd
#Docker build -t af .

#use command  
ENTRYPOINT [ "python3", "Download/cli.py" ]

# run cmd
#sudo docker run -e ACTION=download -it --name af7 -v $HOME/AshaFundingAnalysis:/AshaFundingAnalysis  --net="host" af 