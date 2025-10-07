#include <assert.h>
#include <cstddef>
#include <cstdlib>
#include <stdlib.h>

#include "hashtable.h"


static void h_init(HTab *tab,size_t n){


    assert(n > 0 && ((n-1) & n) == 0);

    tab->tab = (HNode**)calloc(n,sizeof(HNode*));

    tab->mask = n-1;

    tab->size 0;

}

static void h_insert(HTab *htab,HNode *key){


    size_t pos = key->hcode & htab->mask;

    key->next = htab->tab[pos];

    htab->tab[pos] = key;

    htab->size++;

}



static HNode **h_lookup(HTab *htab,HNode *key,bool (*eq)(HNode *,HNode *)){

    if (!htab->tab) {
   
        return NULL;
    }

    size_t pos = key->hcode & htab->mask;
    
    HNode **from = &htab->tab[pos];

    for (HNode *curr;(curr = *from) != NULL; from = &curr->next) {
   
        if (curr->hcode == key->hcode && eq(curr,key)) {

                return from;
        }
        
    }

    return NULL;

}

static HNode *h_detach(HTab *htab,HNode** from){


    HNode *node = *from;

    *from = node->next;

    htab->size--;

    return node;



}

const size_t k_rehashing_work = 128;

static void hm_help_rehasing(HMap *hmap){

    size_t n = 0;
    while (n < k_rehashing_work && hmap->older.size > 0) {


        HNode **from = &hmap->older.tab[hmap->migrate_pos];

        if (!*from) {
       
            hmap->migrate_pos++;
            continue;
        }


        h_insert(&hmap->newer,h_detach(&hmap->older,from));
        n++; 
    }

    if (hmap->older.size == 0 && hmap->older.tab) {

        free(hmap->older.tab);
        hmap->older = HTab{};

    }

}

static void hm_trigger_rehashing(HMap *hmap){

    hmap->older = hmap->newer;

    h_init(&hmap->newer,(hmap->newer.mask+1)*2);

    hmap->migrate_pos = 0;

}

HNode *hm_lookup(HMap *hmap,HNode *key,bool (*eq)(HNode *,HNode *)){


    HNode **from = h_lookup(&hmap->older,key,eq);

    if (!*from) {
   
        from = h_lookup(&hmap->newer,key,eq);
    }


    return from ? *from : NULL;

}
const size_t k_max_load_factor = 8;

void hm_insert(HMap *hmap, HNode *node){


    if (!hmap->newer.tab) {
   
        h_init(&hmap->newer,4);
    
    }

    h_insert(&hmap->newer,node);

    if (!hmap->older.tab) {
   
        size_t threshold = (hmap->newer.mask + 1) * k_max_load_factor;


        if (hmap->newer.size >= threshold) {
    
            hm_trigger_rehashing(hmap);
        }



    }

    hm_help_rehasing(hmap);

}

HNode *hm_delete(HMap *hmap, HNode *key, bool (*eq)(HNode *, HNode *)){

    HNode **from = h_lookup(&hmap->older,key,eq);

    if (*from) {
  
        return h_detach(&hmap->older,from);
    }

    from = h_lookup(&hmap->newer,key,eq);
    
    if (*from) {
   
        return h_detach(&hmap->newer,from);
    }

    return NULL;

}

void hm_clear(HMap *hmap){

    free(hmap->newer.tab);
    free(hmap->older.tab);

    *hmap = HMap{};

}

size_t hm_size(HMap *hmap){

    return hmap->older.size + hmap->newer.size;
}
