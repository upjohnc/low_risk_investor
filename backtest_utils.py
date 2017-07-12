# use core dataframe as one dataset

# create a positions table

# if no open positions
# look for a buy signal
# then set exit, stop, and new buy

# if open positions
# look for exit or stop
# then set sell for all open positions

# look for new buy
# then set new position, update the stop (2*n)

# variable of the last row checked
# loop while row < the last row in the dataframe

# call to get what to do--if none move on else save results
# function should do check and give the updates of the stop, exit, and new buy
