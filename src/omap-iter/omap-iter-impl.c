/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include <stdlib.h>
#include <string.h>
#include "libmobject-store.h"
#include "src/omap-iter/omap-iter-impl.h"
#include "src/util/utlist.h"
#include "src/util/log.h"

void omap_iter_create(mobject_store_omap_iter_t* iter)
{
	*iter = (mobject_store_omap_iter_t)calloc(1, sizeof(**iter));
	(*iter)->ref_count = 1;
}

void omap_iter_free(mobject_store_omap_iter_t iter)
{
	if(!iter) return;
	iter->ref_count -= 1;
	if(iter->ref_count > 0) return;

	omap_iter_node_t elt, tmp;

	DL_FOREACH_SAFE(iter->head, elt, tmp) {
		DL_DELETE(iter->head, elt);
		free((void*)(elt->key));
		free((void*)(elt->value));
		free((void*)(elt));
	}

	free(iter);
}

void omap_iter_incr_ref(mobject_store_omap_iter_t iter)
{
	if(!iter) return;
	iter->ref_count += 1;
}

void omap_iter_append(mobject_store_omap_iter_t iter, 
                      const char* key, const char* val, 
                      size_t val_size)
{
	MOBJECT_ASSERT(iter, "trying to append to a NULL iterator");

	omap_iter_node_t item = (omap_iter_node_t)calloc(1, sizeof(*item));
	item->key             = strdup(key);
	item->key_size        = strlen(key)+1;
	item->value_size      = val_size;
	item->value           = (char*)malloc(val_size);
	memcpy(item->value, val, val_size);

	DL_APPEND(iter->head, item);

	if(iter->current == NULL)
		iter->current = iter->head;

	iter->num_items += 1;
}

int mobject_store_omap_get_next(mobject_store_omap_iter_t iter,
                                char **key,
                                char **val,
                                size_t *len)
{
	if(iter->current == NULL) return -1;

	*key = iter->current->key;
	*val = iter->current->value;
	*len = iter->current->value_size;

	iter->current = iter->current->next;

	return 0;
}

void mobject_store_omap_get_end(mobject_store_omap_iter_t iter)
{
	omap_iter_free(iter);	
}
