import numpy
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import Dropout
from keras.layers import LSTM
from keras.callbacks import ModelCheckpoint
from keras.utils import np_utils
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy import API
import os
import json


filename='tweets.txt'
filepath='C:\Wingz\TrumpAIBot'
path = os.path.join(filepath, filename)

data = open(path,'r').read()
#data = data.lower()

#preprocess data
chars = sorted(list(set(data)))
char_to_int = {c:i for i,c in enumerate(chars)}
int_to_char = {i:c for i,c in enumerate(chars)}
#summarize stats
print 'Total chars in text: ' + str(len(data))
print 'Total unique chars: ' + str(len(chars))

#determine hyperparameters
seq_length = 15 #how many chars the sliding window covers
dataX = []
dataY = []
for i in range(0, len(data) - seq_length, 1):
    seq_in = data[i: i + seq_length] #window of chars/data to predict off of
    seq_out = data[i + seq_length] #actual output, one char 
    #append each list of input chars and its corresponding output char
    dataX.append([char_to_int[char] for char in seq_in])
    dataY.append(char_to_int[seq_out])
#summarize stats
print 'Total number of different training patterns: '+ str(len(dataX))

#reshape input data to [samples, time steps, features]. each sample is a 100x1 matrice
#and one hot encode the output variables
X = numpy.reshape(dataX, (len(dataX), seq_length, 1))
X = X / float(len(chars)) #normalize each integer into a probability 
y = np_utils.to_categorical(dataY) #one hot enconde the output variable 

#build LSTM model
model = Sequential()
model.add(LSTM(256, input_shape=(X.shape[1],X.shape[2]))) #256 memory cells
model.add(Dropout(0.2))
model.add(Dense(y.shape[1],activation='softmax'))
model.compile(loss='categorical_crossentropy', optimizer='adam')
#define the checkpoint
filepath="weights-improvement-{epoch:02d}-{loss:.4f}.hdf5"
checkpoint = ModelCheckpoint(filepath, monitor='loss', verbose=1,save_best_only=True, mode='min')
callbacks_list = [checkpoint]
#fit the model
model.fit(X, y, nb_epoch=20, batch_size = 128, callbacks=callbacks_list)





#load best weights
filename = ''
model.load_weights(filename)
model.compile(loss='categorical_crossentropy', optimizer='adam')
#twitter API information
#information for twitter REST API connection
keys = {
'ACCESS_TOKEN':'2314659403-gMRoUj8gowoiB3nocfbXwIA8JpJ4VxOUcYVXR44',
'ACCESS_SECRET':'EhfdMWiYFL2MeFUgYiid1jl22BJKSuCCBuQABdAPlnlBu',
'CONSUMER_KEY':'u5gk7UZvyGgcWfS7BEt2zYCQP',
'CONSUMER_SECRET':'r8zsmgKBpJEXt4wQcUBpajrX19zgyVkIvj657Ouh1mScCZTKTK'
}

temp = []

class StdOutListener(StreamListener):

    def __init__(self):
        self.input = []
        self.output = []
        
    def on_data(self, data):
        
        #only use data where reply is to my bot
        data = json.loads(data)
        temp.append(data)
        #print json.loads(data)['text']
        tweet_input = []
        if data['is_quote_status'] == False and data['retweeted'] == False:
            text = data['text'][11:]
            if len(text) < 140:
                left_over = 140 - len(text)
                for i in range(left_over):
                    text = text + ' '
            print text
            text = text[0:seq_length]
            tweet_input = [char_to_int[char] for char in text]
            print text
            print tweet_input, len(tweet_input)
            result = '@'
            result = result + data['user']['screen_name'] + ' '
            print result
            for i in range((140-len(result))):
                x = numpy.reshape(tweet_input, (1,seq_length, 1))
                x = x / float(len(chars))
                #print x
                prediction = model.predict(x, verbose=0)
                #print prediction
                index = numpy.argmax(prediction) #grab indices of max arguments
                print int_to_char[index]
                result = result + int_to_char[index]
                #seq_in = [int_to_char[value] for value in tweet_input]
                #print result 
                #print tweet_input, len(tweet_input)
                tweet_input.append(index)
                tweet_input = tweet_input[1:len(tweet_input)] #predict off of new input +1 char
                print tweet_input
                print result                 
            result = result[0:140]
            api.update_status(result)           
            #print data
        #push data into in put stream
        #self.input = [] #clear input stream
        #self.input = data
        #for i in range(data):
        #    x = numpy.reshape(len(data), )
        #predict chars from input stream
        #reply with the predicted chars 140 chars or less to user
        #return True

    def on_error(self, status):
        print status

if __name__ == '__main__':
    l = StdOutListener()
    auth = OAuthHandler(keys['CONSUMER_KEY'], keys['CONSUMER_SECRET'])
    auth.set_access_token(keys['ACCESS_TOKEN'], keys['ACCESS_SECRET'])
    stream = Stream(auth, l) 
    stream.filter(track=['ZengAIBot'])
    api = API(auth)
    
    


