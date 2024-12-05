 import java.util.ArrayList;
 import java.util.List;
 import java.io.*;
import java.lang.*;

 public class IndexNode {
     public Object Indexkeys;
     public List<Object> rowids;
     public boolean isInteriorNode;
     public int leftchild_pageno; // should a unique key(a column in table) points to multiple pointer values

     public IndexNode(Object Indexkeys, List<Object> rowids, int leftchild_pageno) // to add multiple row ids row ids have to be added to the List of integers.
     {
         this.Indexkeys = Indexkeys;
         this.rowids = rowids;
         this.isInteriorNode = false;
         this.leftchild_pageno = -1; // default points to parent
     }
     public IndexNode(Object Indexkeys, List<Integer>rowids){
         this(Indexkeys, rowids, -1);
         this.isInteriorNode = false;
     }

//Define how the interior and leaf node behaves?


     public class Btree {
         private IndexNode root;
         private int order; // max degree

         public Btree(int order) {
             this.order = order;
             this.root = new IndexNode(null, new ArrayList<>(), -1);
         }

         // Search operations, searches for a key in the tree

         public IndexNode search(IndexNode node, Object keyTosearch) {
             int i = 0;
             while (i < node.Indexkeys.size() && keyTosearch.compareTo(node.Indexkeys) > 0) {
                 i++;
             }
             if (i < node.Indexkeys.size() && keyTosearch.compareTO(node.Indexkeys) == 0) {
                 return node; // key found
             }
             if (node.isInteriorNode) {
                 return search(new IndexNode(node.Indexkeys, node.rowids, node.leftchild_pageno), keyTosearch);
             }
            return null;
         }
         // Inserting a key
         public void Insert(Object key, int rowid){
             if (root.Indexkeys.size() == order -1){ //check how indexkeys can be made as list to store many keys.
                IndexNode newRoot = new IndexNode(null, new ArrayList<>());
                newRoot.isInteriorNode = true;
                newRoot.leftchild_pageno = 0;
                split(newRoot,0);
                root = newRoot;

             }
            insertNonFull(root,key, rowid);
         }
        private void insertNonFull(IndexNode node,Object key,int rowid){
            int i = node.Indexkeys.size() -1;
            if (node.isInteriorNode){
                // find appropriate child node to insert the key
                while(i >= 0 && ((Comparable) key).compareTo(node.Indexkeys) < 0) {
                    i--;
                }
                i++;

                // if the child node is full, split
                if (node.Indexkeys.size() == order -1){
                    split(node,i);
                    if (((Comparable) key).compareTo(node.Indexkeys) > 0){
                        i++;

                    }
                }
                insertNonFull(new IndexNode(node.Indexkeys,node.rowids,node.leftchild_pageno));

            } else {
                // Insert into the leaf node
                while (i >= 0 && ((Comparable) key).compareTo(node.Indexkeys) < 0 ){
                    i--;

                }
                node.Indexkeys = key;
                node.rowids.add(rowid);
            }

        }
        private void split(IndexNode parent, int index){
             IndexNode fullNode = parent.Indexkeys.get(index);
             IndexNode newnode = new IndexNode(fullNode.Indexkeys, new ArrayList<>());
             parent.Indexkeys.add(index,fullNode.Indexkeys);
             parent.Indexkeys.add(index+1,newnode.Indexkeys);
        }


        public void delete(Object key){
             delete(root, key);


        }
     }  private void delete(IndexNode node, Object key){
         int i =0;
         while (i < node.Indexkeys.size() && (Comparable) key).compareTo(node.Indexkeys) > 0 ){
            i++;
         }
         if (i < node.Indexkeys.size() && ((Comparable) key).compareTo(node.Indexkeys) == 0){
             if (!node.isInteriorNode){
                 node.Indexkeys.remove(i);
             }
             else if(node.isInteriorNode){
                 delete(new IndexNode(node.Indexkeys, node.rowids,node.leftchild_pageno), key);

             }

         }

     }

 }
