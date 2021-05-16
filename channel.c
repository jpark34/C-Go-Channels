#include "channel.h"

//Function to initialize shared data, locks, mutex
static int data_init(channel_t* channel){

    //initialize mutex
    if (pthread_mutex_init(&channel->mutex, NULL) != 0) return GEN_ERROR;

    //initialize condition for send, if init doesn't work destroy previous initialized mutex
    if (pthread_cond_init(&channel->send_cond, NULL) != 0) {
        pthread_mutex_destroy(&channel->mutex);
        return GEN_ERROR;
    }

    //initialize condition for receive, if init doesn't work destroy previous mutex and condition
    if (pthread_cond_init(&channel->receive_cond, NULL) != 0) {
        pthread_mutex_destroy(&channel->mutex);
        pthread_cond_destroy(&channel->send_cond);
        return GEN_ERROR;
    }

    if (pthread_mutex_init(&channel->select_mutex, NULL) != 0) {
        pthread_mutex_destroy(&channel->mutex);
        pthread_cond_destroy(&channel->send_cond);
        pthread_cond_destroy(&channel->receive_cond);
        return GEN_ERROR;
    }

    if (pthread_cond_init(&channel->select_cond, NULL) != 0) {
        pthread_mutex_destroy(&channel->mutex);
        pthread_cond_destroy(&channel->send_cond);
        pthread_cond_destroy(&channel->receive_cond);
        pthread_mutex_destroy(&channel->select_mutex);
        return GEN_ERROR;
    }

    //initialize other basic data required for the channel
    channel->channel_closed = 0;
    channel->buffer = NULL;
    channel->data = NULL;
    channel->send_wait = 0;
    channel->receive_wait = 0;
    channel->select_wait = 0;

    return 0;
}

//Function to give the buffer memory and give it a value
static int buffered_init(channel_t* channel, size_t size){

    //create the buffer with the given size
    buffer_t* buffer1 = buffer_create(size);

    if(!(buffer1)) return GEN_ERROR;

    //initialize all the data required for the buffer
    if (data_init(channel) != 0){
        buffer_free(buffer1);
        return GEN_ERROR;
    }

    channel->buffer = buffer1;

    return 0;
}

//Function to check if the channel is open
//Return 1 if channel is closed, 0 if channel is open
//take this out eventually
static int check_open(channel_t* channel){

    pthread_mutex_lock(&channel->mutex);

    int is_closed = channel->channel_closed;

    pthread_mutex_unlock(&channel->mutex);

    return is_closed;
}

// Creates a new channel with the provided size and returns it to the caller
// A 0 size indicates an unbuffered channel, whereas a positive size indicates a buffered channel
channel_t* channel_create(size_t size)
{
    /* IMPLEMENT THIS */

    //give the channel memory
    channel_t* channel = (channel_t*) malloc(sizeof(channel_t));

    if (!channel) return NULL;

    //indicates that a buffered channel is needed
    if (size > 0){                              
        //initialize the buffer and check to make sure it worked, if it doesn't initialize then free the memory given to the channel
        if (buffered_init(channel, size) != 0){
            free(channel);
            return NULL;
        }
    }

    //indicates that an unbuffered channel is needed
    else {                                     
        //initialize the buffer and check to make sure it worked, if it doesn't initialize then free the memory give to the channel
        if (data_init(channel) != 0){
            free(channel);
            return NULL;
        }
    }

    return channel;
}

// Writes data to the given channel
// This is a blocking call i.e., the function only returns on a successful completion of send
// In case the channel is full, the function waits till the channel has space to write the new data
// Returns SUCCESS for successfully writing data to the channel,
// CLOSED_ERROR if the channel is closed, and
// GEN_ERROR on encountering any other generic error of any sort
enum channel_status channel_send(channel_t *channel, void* data)
{
    /* IMPLEMENT THIS */

    pthread_mutex_lock(&channel->mutex);

    //check if the chanel was closed
    if (channel->channel_closed == 1) return CLOSED_ERROR;    

    //check to see if there is room in the buffer to send something to it
    while ((buffer_current_size(channel->buffer)) == (buffer_capacity(channel->buffer))){
        //block until there is room in the buffer to send something to it
        channel->send_wait++;
        pthread_cond_wait(&channel->send_cond, &channel->mutex);
        channel->send_wait--;

        //check to see if the channel is closed
        if (channel->channel_closed == 1) {
            pthread_mutex_unlock(&channel->mutex);
            return CLOSED_ERROR;
        }
    }

    //check to see if the channel was closed
    if (channel->channel_closed == 1) {
        pthread_mutex_unlock(&channel->mutex);
        return CLOSED_ERROR;
    }

    //add the data to the buffer, unlock the mutex if data could not be added to the buffer
    if ((buffer_add(channel->buffer, data)) == -1) {
        pthread_mutex_unlock(&channel->mutex);
        return GEN_ERROR;
    }

    //wake a receive thread if there are waiting receive threads
    if (channel->receive_wait > 0){
        pthread_cond_signal(&channel->receive_cond);
    }

    if (channel->select_wait > 0) {
        pthread_cond_signal(&channel->select_cond);
    }

    pthread_mutex_unlock(&channel->mutex);

    return SUCCESS;
}

// Reads data from the given channel and stores it in the function’s input parameter, data (Note that it is a double pointer).
// This is a blocking call i.e., the function only returns on a successful completion of receive
// In case the channel is empty, the function waits till the channel has some data to read
// Returns SUCCESS for successful retrieval of data,
// CLOSED_ERROR if the channel is closed, and
// GEN_ERROR on encountering any other generic error of any sort
enum channel_status channel_receive(channel_t* channel, void** data)
{
    /* IMPLEMENT THIS */

    pthread_mutex_lock(&channel->mutex);

    //check if chanel was closed
    if (channel->channel_closed == 1) return CLOSED_ERROR;

    //check to see if there are messages in the buffer to receive
    while ((buffer_current_size(channel->buffer)) == 0){
        //block until there is something in the buffer to receive
        channel->receive_wait++;
        pthread_cond_wait(&channel->receive_cond, &channel->mutex);
        channel->receive_wait--;

        //check to see if the channel was closed
        if (channel->channel_closed == 1) {
            pthread_mutex_unlock(&channel->mutex);
            return CLOSED_ERROR;
        }
    }

    //check to see if the channel was closed
    if (channel->channel_closed == 1) {
        pthread_mutex_unlock(&channel->mutex);
        return CLOSED_ERROR;
    }

    //removes the value from the buffer in FIFO order and stores that value in data
    //unlock the mutex if a value could not be removed from the buffer
    if ((buffer_remove(channel->buffer, data)) == -1) {
        pthread_mutex_unlock(&channel->mutex);
        return GEN_ERROR;
    }

    //wake a send thread if there are waiting send threads
    if (channel->send_wait > 0){
        pthread_cond_signal(&channel->send_cond);
    }

    if (channel->select_wait > 0) {
        pthread_cond_signal(&channel->select_cond);
    }

    pthread_mutex_unlock(&channel->mutex);

    return SUCCESS;
}

// Writes data to the given channel
// This is a non-blocking call i.e., the function simply returns if the channel is full
// Returns SUCCESS for successfully writing data to the channel,
// CHANNEL_FULL if the channel is full and the data was not added to the buffer,
// CLOSED_ERROR if the channel is closed, and
// GEN_ERROR on encountering any other generic error of any sort
enum channel_status channel_non_blocking_send(channel_t* channel, void* data)
{
    /* IMPLEMENT THIS */

    //check to see if the channel is closed
    if (check_open(channel) == 1) return CLOSED_ERROR;

    pthread_mutex_lock(&channel->mutex);

    //if there is no room in the buffer, return that the channel is full
    if ((buffer_current_size(channel->buffer)) == (buffer_capacity(channel->buffer))) {
        pthread_mutex_unlock(&channel->mutex);
        return CHANNEL_FULL;
    }

    if (channel->channel_closed == 1) {
        pthread_mutex_unlock(&channel->mutex);
        return CLOSED_ERROR;
    }

    //we know there is room in the buffer so add to the buffer
    //release the lock if buffer_add does not work
    if ((buffer_add(channel->buffer, data)) == -1) {
        pthread_mutex_unlock(&channel->mutex);
        return GEN_ERROR;
    }

    if (channel->select_wait > 0) {
        pthread_cond_signal(&channel->select_cond);
    }

    pthread_mutex_unlock(&channel->mutex);    

    return SUCCESS;
}

// Reads data from the given channel and stores it in the function’s input parameter data (Note that it is a double pointer)
// This is a non-blocking call i.e., the function simply returns if the channel is empty
// Returns SUCCESS for successful retrieval of data,
// CHANNEL_EMPTY if the channel is empty and nothing was stored in data,
// CLOSED_ERROR if the channel is closed, and
// GEN_ERROR on encountering any other generic error of any sort
enum channel_status channel_non_blocking_receive(channel_t* channel, void** data)
{
    /* IMPLEMENT THIS */

    //check to see if the channel has been closed
    if (channel->channel_closed == 1) return CLOSED_ERROR;

    pthread_mutex_lock(&channel->mutex);

    //if the channel is empty return that the channel is empty
    if ((buffer_current_size(channel->buffer)) == 0) {
        pthread_mutex_unlock(&channel->mutex);
        return CHANNEL_EMPTY;
    }

    //removes the value from the buffer in FIFO order and stores that value in data
    if ((buffer_remove(channel->buffer, data)) == -1) {
        pthread_mutex_unlock(&channel->mutex);
        return GEN_ERROR;
    }

    //wake a send thread if there are waiting send threads
    if (channel->send_wait > 0){
        pthread_cond_signal(&channel->send_cond);
    }

    if (channel->select_wait > 0) {
        pthread_cond_signal(&channel->select_cond);
    }

    pthread_mutex_unlock(&channel->mutex);

    return SUCCESS;
}

// Closes the channel and informs all the blocking send/receive/select calls to return with CLOSED_ERROR
// Once the channel is closed, send/receive/select operations will cease to function and just return CLOSED_ERROR
// Returns SUCCESS if close is successful,
// CLOSED_ERROR if the channel is already closed, and
// GEN_ERROR in any other error case
enum channel_status channel_close(channel_t* channel)
{
    /* IMPLEMENT THIS */

    pthread_mutex_lock(&channel->mutex);

    //check to see if channel is already closed
    if (channel->channel_closed == 1) {
        pthread_mutex_unlock(&channel->mutex);
        return CLOSED_ERROR;
    }

    //if the channel is not closed then set the flag to close the channel
    //and let all waiting channels know that it has been closed
    else {
        channel->channel_closed = 1;                                            
        pthread_cond_broadcast(&channel->send_cond);
        pthread_cond_broadcast(&channel->receive_cond);
        pthread_cond_broadcast(&channel->select_cond);
    }

    pthread_mutex_unlock(&channel->mutex);

    return SUCCESS;
}

// Frees all the memory allocated to the channel
// The caller is responsible for calling channel_close and waiting for all threads to finish their tasks before calling channel_destroy
// Returns SUCCESS if destroy is successful,
// DESTROY_ERROR if channel_destroy is called on an open channel, and
// GEN_ERROR in any other error case
enum channel_status channel_destroy(channel_t* channel)
{
    /* IMPLEMENT THIS */

    //check to see if the channel has been closed already
    if (channel->channel_closed == 0) return DESTROY_ERROR;

    //if we are dealing with a buffered channel then free that buffer
    //will help once unbuffered is implemented
    if (channel->buffer != NULL) {
        buffer_free(channel->buffer);
    }

    //destroy the mutex
    pthread_mutex_destroy(&channel->mutex);
    pthread_mutex_destroy(&channel->select_mutex);

    //destroy the two condition variables
    pthread_cond_destroy(&channel->send_cond);
    pthread_cond_destroy(&channel->receive_cond);
    pthread_cond_destroy(&channel->select_cond);

    //free the memory given to the channel
    free(channel);

    return SUCCESS;
}

// Takes an array of channels, channel_list, of type select_t and the array length, channel_count, as inputs
// This API iterates over the provided list and finds the set of possible channels which can be used to invoke the required operation (send or receive) specified in select_t
// If multiple options are available, it selects the first option and performs its corresponding action
// If no channel is available, the call is blocked and waits till it finds a channel which supports its required operation
// Once an operation has been successfully performed, select should set selected_index to the index of the channel that performed the operation and then return SUCCESS
// In the event that a channel is closed or encounters any error, the error should be propagated and returned through select
// Additionally, selected_index is set to the index of the channel that generated the error
enum channel_status channel_select(select_t* channel_list, size_t channel_count, size_t* selected_index)
{
    /* IMPLEMENT THIS */

    *selected_index = 0;
    size_t i = 0;

    //check all the channels in the list to see if any are closed
    //MAY NEED TO SET SELECTED_INDEX BEFORE RETURN
    while (i < channel_count) {
        if (channel_list[i].channel->channel_closed == 1) {
            *selected_index = i;
            return CLOSED_ERROR;
        }
        i++;
    }

    i = 0;
    int send = 0;
    int receive = 0;

    //iterate over channel_list
    while (i < channel_count) {
        pthread_mutex_lock(&channel_list[i].channel->select_mutex);
        //check if the channel is closed
        if (channel_list[i].channel->channel_closed == 1) {
            pthread_mutex_unlock(&channel_list[i].channel->select_mutex);
            return CLOSED_ERROR;
        }

        //check if the channel should RECV
        if (channel_list[i].dir == RECV) {
            send = 1;
            //check if buffer has items
            if ((buffer_current_size(channel_list[i].channel->buffer) > 0) && (channel_list[i].channel->channel_closed == 0)) {
                *selected_index = i;
                send = 0;
                pthread_mutex_unlock(&channel_list[i].channel->select_mutex);
                return channel_receive (channel_list[i].channel, &(channel_list[i].data));
            }
        }

        //check if the channel should SEND
        else if (channel_list[i].dir == SEND) {
            receive = 1;
            //check to make sure the buffer is not full
            if ((buffer_current_size(channel_list[i].channel->buffer)) != (buffer_capacity(channel_list[i].channel->buffer)) && (channel_list[i].channel->channel_closed == 0)) {
                *selected_index = i;
                receive = 0;
                pthread_mutex_unlock(&channel_list[i].channel->select_mutex);
                return channel_send(channel_list[i].channel, channel_list[i].data);
            }
        }

        i++;

        //no channels have done their job yet, block until signaled by send or receive
        if (i == channel_count) {
            i--;

            if ((receive && channel_list[i].channel->receive_wait == 0 && (buffer_current_size(channel_list[i].channel->buffer)) == (buffer_capacity(channel_list[i].channel->buffer))) ||
            (send && channel_list[i].channel->send_wait == 0 && (buffer_current_size(channel_list[i].channel->buffer) == 0))) {
                channel_list[i].channel->select_wait++;
                pthread_cond_wait(&channel_list[i].channel->select_cond, &channel_list[i].channel->select_mutex);
                channel_list[i].channel->select_wait--;
            }
            i = 0;
        }

        pthread_mutex_unlock(&channel_list[i].channel->select_mutex);
    }

    return SUCCESS;
}

//need to destroy mutex and cond in select