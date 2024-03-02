#include "channel.h"


void Send_Helper(channel_t *channel, void* data)
{
    buffer_add(channel->buffer, data);
    if(channel->size != 0){
        if(buffer_current_size(channel->buffer) == channel->size){
            channel->status = CHANNEL_FULL;
        }else if(channel->status == CHANNEL_EMPTY){
            channel->status = OPEN;
        }
    }

    for (list_node_t *i = list_head(channel->channel_selects_recvive); i != NULL; i = list_next(i)) {
        pthread_mutex_lock(((select_t*)list_data(i))->mutex);
        pthread_cond_signal(((select_t*)list_data(i))->cond);
        pthread_mutex_unlock(((select_t*)list_data(i))->mutex);
    }
    pthread_cond_signal(&(channel->cond_user));
}

void Receive_Helper(channel_t* channel, void** data)
{
    buffer_remove(channel->buffer, data);
    if(channel->size != 0){
        if(buffer_current_size(channel->buffer) == 0){
            channel->status = CHANNEL_EMPTY;
        }else if(channel->status == CHANNEL_EMPTY){
            channel->status = OPEN;
        }
    }
    for (list_node_t *i = list_head(channel->channel_selects_send); i != NULL; i = list_next(i)) {
        pthread_mutex_lock(((select_t*)list_data(i))->mutex);
        pthread_cond_signal(((select_t*)list_data(i))->cond);
        pthread_mutex_unlock(((select_t*)list_data(i))->mutex);
    }
    pthread_cond_signal(&(channel->cond));
    if(channel->size == 0)
        pthread_cond_signal(&(channel->wait));
}

// Creates a new channel with the provided size and returns it to the caller
// A 0 size indicates an unbuffered channel, whereas a positive size indicates a buffered channel
channel_t* channel_create(size_t size)
{
    /* IMPLEMENT THIS */
    channel_t *channel = malloc(sizeof(channel_t));
    if(size == 0){
        channel->buffer = buffer_create(1);
        channel->status = CHANNEL_FULL;
    }else{
        channel->buffer = buffer_create(size);
        channel->status = CHANNEL_EMPTY;
    }
    pthread_mutex_init(&(channel->mutex), NULL);
    pthread_cond_init(&(channel->cond), NULL);
    pthread_cond_init(&(channel->wait), NULL);
    pthread_cond_init(&(channel->cond_user), NULL);
    channel->size = size;
    channel->channel_selects_recvive = list_create();
    channel->channel_selects_send = list_create();
    channel->wait_num = 0;
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
    if(channel->size == 0){
        if (channel == NULL){
            return GENERIC_ERROR;
        }
        pthread_mutex_lock(&(channel->mutex));
            while(buffer_current_size(channel->buffer) == 1 && channel->status == CHANNEL_FULL){
                pthread_cond_wait(&(channel->cond), &(channel->mutex));
            }
        if(channel->status == CLOSED_ERROR){
            pthread_mutex_unlock(&(channel->mutex));
            return CLOSED_ERROR;
        }else {
            Send_Helper(channel, data);
            pthread_cond_wait(&(channel->wait), &(channel->mutex));
            pthread_mutex_unlock(&(channel->mutex));
            return SUCCESS;
        }
        
    }    
    pthread_mutex_lock(&(channel->mutex));
    while(channel->status == CHANNEL_FULL){
        pthread_cond_wait(&(channel->cond), &(channel->mutex));
    }
    if(channel->status == CLOSED_ERROR){
        pthread_mutex_unlock(&(channel->mutex));
        return CLOSED_ERROR;
    }
    else{   
        Send_Helper(channel, data);
        pthread_mutex_unlock(&(channel->mutex));
        return SUCCESS;
    }
}


// Reads data from the given channel and stores it in the function's input parameter, data (Note that it is a double pointer)
// This is a blocking call i.e., the function only returns on a successful completion of receive
// In case the channel is empty, the function waits till the channel has some data to read
// Returns SUCCESS for successful retrieval of data,
// CLOSED_ERROR if the channel is closed, and
// GEN_ERROR on encountering any other generic error of any sort
enum channel_status channel_receive(channel_t* channel, void** data)
{
    /* IMPLEMENT THIS */
    if(channel->size == 0)
        {
            pthread_mutex_lock(&(channel->mutex));
            channel->wait_num ++;
            if(list_count(channel->channel_selects_send) != 0){
                for (list_node_t *i = list_head(channel->channel_selects_send); i != NULL; i = list_next(i)) {
                    pthread_mutex_lock(((select_t*)list_data(i))->mutex);
                    pthread_cond_signal(((select_t*)list_data(i))->cond);
                    pthread_mutex_unlock(((select_t*)list_data(i))->mutex);
                }
            }
            while (buffer_current_size(channel->buffer) == 0 && channel->status == CHANNEL_FULL){
                pthread_cond_wait(&(channel->cond_user), &(channel->mutex));
            }
            if(channel->status == CLOSED_ERROR){
                pthread_mutex_unlock(&(channel->mutex));
                return CLOSED_ERROR;
            }
            Receive_Helper(channel,data);
            channel->wait_num --;
            pthread_mutex_unlock(&(channel->mutex));
            return SUCCESS;
        }
    pthread_mutex_lock(&(channel->mutex));
    while(channel->status == CHANNEL_EMPTY){
        pthread_cond_wait(&(channel->cond_user), &(channel->mutex));
    }
    if(channel->status == CLOSED_ERROR){
        return CLOSED_ERROR;
    }else{
        return SUCCESS;
    }
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
    pthread_mutex_lock(&(channel->mutex));
    if(channel->size == 0){
        if(buffer_current_size(channel->buffer) == 1 && channel->status == CHANNEL_FULL){
        return CHANNEL_FULL;
        }
        if(channel->status == CLOSED_ERROR){
            return CLOSED_ERROR;
        }
        if(channel->wait_num == 0 && list_count(channel->channel_selects_recvive) == 0){
            return CHANNEL_FULL;
        }
        Send_Helper(channel, data);
        pthread_mutex_unlock(&(channel->mutex));
        return SUCCESS;
    }
    if(channel->status == CHANNEL_FULL){
        return CHANNEL_FULL;
    }
    if(channel->status == CLOSED_ERROR){
        return CLOSED_ERROR;
    }
    Send_Helper(channel, data);
    pthread_mutex_unlock(&(channel->mutex));
    return SUCCESS;
}

// Reads data from the given channel and stores it in the function's input parameter data (Note that it is a double pointer)
// This is a non-blocking call i.e., the function simply returns if the channel is empty
// Returns SUCCESS for successful retrieval of data,
// CHANNEL_EMPTY if the channel is empty and nothing was stored in data,
// CLOSED_ERROR if the channel is closed, and
// GEN_ERROR on encountering any other generic error of any sort
enum channel_status channel_non_blocking_receive(channel_t* channel, void** data)
{
    pthread_mutex_lock(&(channel->mutex));
    if(channel->size == 0){
        while (buffer_current_size(channel->buffer) == 0 && channel->status == CHANNEL_FULL){
            if(list_count(channel->channel_selects_send) != 0){
                channel->wait_num ++;
                for (list_node_t *i = list_head(channel->channel_selects_send); i != NULL; i = list_next(i)) {
                    pthread_mutex_lock(((select_t*)list_data(i))->mutex);
                    pthread_cond_signal(((select_t*)list_data(i))->cond);
                    pthread_mutex_unlock(((select_t*)list_data(i))->mutex);
                }
                pthread_cond_wait(&(channel->cond_user), &(channel->mutex));
                channel->wait_num--;
            }else
                return CHANNEL_EMPTY;
        }
        if(channel->status == CLOSED_ERROR){
            return CLOSED_ERROR;
        }
        Receive_Helper(channel, data);
        return SUCCESS;
    }       
    if(channel->status == CHANNEL_EMPTY){
        return CHANNEL_EMPTY;
    }
    if(channel->status == CLOSED_ERROR){
        return CLOSED_ERROR;
    }
    Receive_Helper(channel, data);
    pthread_mutex_unlock(&(channel->mutex));
    return SUCCESS;
}

// Closes the channel and informs all the blocking send/receive/select calls to return with CLOSED_ERROR
// Once the channel is closed, send/receive/select operations will cease to function and just return CLOSED_ERROR
// Returns SUCCESS if close is successful,
// CLOSED_ERROR if the channel is already closed, and
// GEN_ERROR in any other error case
enum channel_status channel_close(channel_t* channel)
{
   pthread_mutex_lock(&(channel->mutex));
    if(channel->status == CLOSED_ERROR){
        pthread_mutex_unlock(&(channel->mutex));
        return CLOSED_ERROR;
    }
    channel->status = CLOSED_ERROR;
    pthread_cond_broadcast(&(channel->cond));
    pthread_cond_broadcast(&(channel->cond_user));
    for (list_node_t *n = list_head(channel->channel_selects_send); n != NULL ; n = list_next(n)) {
        pthread_mutex_lock(((select_t*)list_data(n))->mutex);
        pthread_cond_signal(((select_t*)list_data(n))->cond);
        pthread_mutex_unlock(((select_t*)list_data(n))->mutex);
    }
    for (list_node_t *n = list_head(channel->channel_selects_recvive); n != NULL ; n = list_next(n)) {
        pthread_mutex_lock(((select_t*)list_data(n))->mutex);
        pthread_cond_signal(((select_t*)list_data(n))->cond);
        pthread_mutex_unlock(((select_t*)list_data(n))->mutex);
    }
    pthread_mutex_unlock(&(channel->mutex));
    return SUCCESS;
}

// Frees all the memory allocated to the channel
// The caller is responsible for calling channel_close and waiting for all threads to finish their tasks before calling channel_destroy
// Returns SUCCESS if destroy is successful,
// DESTROY_ERROR if channel_destroy is called on an open channel, and
// GEN_ERROR in any other error case
enum channel_status channel_destroy(channel_t* channel)
{
    if(channel->status != CLOSED_ERROR){
        return DESTROY_ERROR;
    }
    pthread_mutex_destroy(&(channel->mutex));
    pthread_cond_destroy(&(channel->cond));
    pthread_cond_destroy(&(channel->cond_user));
    pthread_cond_destroy(&(channel->wait));
    list_destroy(channel->channel_selects_send);
    list_destroy(channel->channel_selects_recvive);
    buffer_free(channel->buffer);
    free(channel);
    return SUCCESS;
}

void unlock_channel_list(select_t* channel_list, size_t channel_count){
    for (int i = 0; i < channel_count; ++i) {
        int duplicate = 0;
        for (int j = 0; j < i; ++j) {
            if(channel_list[j].channel == channel_list[i].channel){
                duplicate = 1;
            }
        }

        if(duplicate == 0){
            pthread_mutex_unlock(&(channel_list[i].channel->mutex));
        }
    }
}

void try_lock_channels(select_t* channel_list, size_t channel_count){
    while(1){
        int succeed = 1;
        for (int i = 0; i < channel_count; ++i) {
            int duplicate = 0;
            for (int j = 0; j < i; ++j) {
                if(channel_list[j].channel == channel_list[i].channel){
                    duplicate = 1;
                }
            }
            if(duplicate == 0){
                if(pthread_mutex_trylock(&(channel_list[i].channel->mutex)) != 0){
                    succeed = 0;
                    unlock_channel_list(channel_list, (size_t)i);
                    break;
                }
            }
        }
        if(succeed == 1)
            return;
    }
}

// Takes an array of channels (channel_list) of type select_t and the array length (channel_count) as inputs
// This API iterates over the provided list and finds the set of possible channels which can be used to invoke the required operation (send or receive) specified in select_t
// If multiple options are available, it selects the first option and performs its corresponding action
// If no channel is available, the call is blocked and waits till it finds a channel which supports its required operation
// Once an operation has been successfully performed, select should set selected_index to the index of the channel that performed the operation and then return SUCCESS
// In the event that a channel is closed or encounters any error, the error should be propagated and returned through select
// Additionally, selected_index is set to the index of the channel that generated the error
enum channel_status channel_select(select_t* channel_list, size_t channel_count, size_t* selected_index)
{
    /* IMPLEMENT THIS */
    pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
    pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

    while(1){
        try_lock_channels(channel_list, channel_count);
        for (int i = 0; i < channel_count; ++i) {
            //pthread_mutex_lock(&(channel_list[i].channel->mutex));
            list_t *l;
            if(channel_list[i].dir == SEND)
                l = channel_list[i].channel->channel_selects_send;
            else
                l = channel_list[i].channel->channel_selects_recvive;
            list_node_t* n = list_find(l, channel_list);
            if(n != NULL){
                list_remove(l, n);
            }
            //pthread_mutex_unlock(&(channel_list[i].channel->mutex));
        }

        for (int i = 0; i < channel_count; ++i) {
            if(channel_list[i].dir == SEND){
                enum channel_status s = channel_non_blocking_send(channel_list[i].channel, channel_list[i].data);
                //printf("thread %lx: send to channel %d, %d\n", pthread_self(), i, s);
                //fflush(stdout);
                //enum channel_status s = channel_non_blocking_send(channel_list[i].channel, channel_list[i].data);
                if(s != CHANNEL_FULL){
                    *selected_index = (size_t)i;
                    //pthread_mutex_unlock(&mutex);
                    unlock_channel_list(channel_list, channel_count);
                    pthread_mutex_destroy(&(mutex));
                    pthread_cond_destroy(&(cond));
                    return s;
                }
            }else{
                enum channel_status s = channel_non_blocking_receive(channel_list[i].channel, &(channel_list[i].data));
                //printf("thread %lx: recv from channel %d, %d\n", pthread_self(), i, s);
                //fflush(stdout);
                //enum channel_status s = channel_non_blocking_receive(channel_list[i].channel, &(channel_list[i].data));
                if(s != CHANNEL_EMPTY){
                    *selected_index = (size_t)i;
                    //pthread_mutex_unlock(&mutex);
                    unlock_channel_list(channel_list, channel_count);
                    pthread_mutex_destroy(&(mutex));
                    pthread_cond_destroy(&(cond));
                    return s;
                }
                //pthread_cond_signal(&(channel_list[i].channel->generator_cond));
            }
        }

        //printf("Thread %lx wait in select: ", pthread_self());
        pthread_mutex_lock(&mutex);
        channel_list->mutex = &mutex;
        channel_list->cond = &cond;
        for (int i = 0; i < channel_count; ++i) {
            //pthread_mutex_lock(&(channel_list[i].channel->mutex));
            if(channel_list[i].dir == RECV){
                list_insert(channel_list[i].channel->channel_selects_recvive, channel_list);
                //printf(" RECV on channel %lx", (unsigned long)(channel_list[i].channel));
            }
            else{
                list_insert(channel_list[i].channel->channel_selects_send, channel_list);
                //printf(" SEND on channel %lx", (unsigned long)(channel_list[i].channel));
            }


            /*if(channel_list[i].dir == RECV && channel_list[i].channel->status != CHANNEL_EMPTY){
                printf(" recv when have data %d", channel_list[i].channel->status);
                fflush(stdout);
            }else if(channel_list[i].dir == SEND && channel_list[i].channel->status != CHANNEL_FULL){
                printf(" send when have no %d", channel_list[i].channel->status);
                fflush(stdout);
            }*/
            //pthread_mutex_unlock(&(channel_list[i].channel->mutex));
        }
        //printf("\n");
        //fflush(stdout);

        unlock_channel_list(channel_list, channel_count);
        pthread_cond_wait(&cond, &mutex);
        pthread_mutex_unlock(&mutex);
        //printf("Thread %lx awake in select\n", pthread_self());
    }
}