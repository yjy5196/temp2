#include <stdlib.h>
#include "linked_list.h"

// Creates and returns a new list
list_t* list_create()
{
    list_t* list = (list_t*)malloc(sizeof(list_t));
    if (list) {
        list->head = NULL;
        list->tail = NULL;
        list->count = 0;
    }
    return list;
}

// Destroys a list
void list_destroy(list_t* list)
{
    if (!list)
        return;

    list_node_t* current = list->head;
    while (current) {
        list_node_t* next = current->next;
        free(current);
        current = next;
    }
    free(list);
}

// Returns head of the list
list_node_t* list_head(list_t* list)
{
    if (!list)
        return NULL;
    return list->head;
}

// Returns tail of the list
list_node_t* list_tail(list_t* list)
{
    if (!list)
        return NULL;
    return list->tail;
}

// Returns next element in the list
list_node_t* list_next(list_node_t* node)
{
    if (!node)
        return NULL;
    return node->next;
}

// Returns prev element in the list
list_node_t* list_prev(list_node_t* node)
{
    if (!node)
        return NULL;
    return node->prev;
}

// Returns end of the list marker
list_node_t* list_end(list_t* list)
{
    if (!list)
        return NULL;
    return NULL; 
}

// Returns data in the given list node
void* list_data(list_node_t* node)
{
    if (!node)
        return NULL;
    return node->data;
}

// Returns the number of elements in the list
size_t list_count(list_t* list)
{
    if (!list)
        return 0;
    return list->count;
}

// Finds the first node in the list with the given data
// Returns NULL if data could not be found
list_node_t* list_find(list_t* list, void* data)
{
    if (!list)
        return NULL;

    list_node_t* current = list->head;
    while (current) {
        if (current->data == data)
            return current;
        current = current->next;
    }
    return NULL;
}

// Inserts a new node in the list with the given data
// Returns new node inserted
list_node_t* list_insert(list_t* list, void* data)
{
    if (!list)
        return NULL;

    list_node_t* newNode = (list_node_t*)malloc(sizeof(list_node_t));
    if (!newNode)
        return NULL;

    newNode->data = data;
    newNode->next = NULL;
    newNode->prev = list->tail;

    if (list->tail)
        list->tail->next = newNode;

    list->tail = newNode;
    if (!list->head)
        list->head = newNode;

    list->count++;
    return newNode;
}

// Removes a node from the list and frees the node resources
void list_remove(list_t* list, list_node_t* node)
{
    if (!list || !node)
        return;

    if (node->prev)
        node->prev->next = node->next;
    else
        list->head = node->next;

    if (node->next)
        node->next->prev = node->prev;
    else
        list->tail = node->prev;

    free(node);
    list->count--;
}
