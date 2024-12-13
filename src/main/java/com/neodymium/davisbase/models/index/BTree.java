package com.neodymium.davisbase.models;

import com.neodymium.davisbase.models.index.CellHeader;
import com.neodymium.davisbase.models.table.TableCell;
import lombok.Data;
package com.neodymium.davisbase.models.table;
import com.neodymium.davisbase.constants.enums.PageTypes;
import com.neodymium.davisbase.error.DavisBaseException;
import com.neodymium.davisbase.models.index.Cell;
import com.neodymium.davisbase.models.index.Page;
import lombok.AllArgsConstructor;

import javax.print.attribute.Attribute;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.concurrent.locks.Condition;

import static com.neodymium.davisbase.constants.Constants.PAGE_SIZE;

import static org.graalvm.compiler.asm.sparc.SPARCAssembler.Fcn.Page;


@Data
public class BTree() {
    private final int pageSize;  // page size
    private final RandomAccessFile indexFile; //  file to read
    private CellHeader cellHeader;


    public void create() throws IOException {

        // this is the page where .nx files will be written and hence is created empty
        Page rootPage = new Page(pageSize, 0, PageTypes.LEAF.getValue(), //starting root page.
                (short) 0, (short) 0xFFFF, (short) 0xFFFF);
        writePage(rootPage,0);
    }

    public void insert(Cell cell) throws IOException {

        HashMap<Objects, Integer> LeftNode = new HashMap<>();
        HashMap<Objects, Integer> RightNode = new HashMap<>();
        Page rootpage = getRootPage(); // returns the root page with data
        try {
            rootpage.insert(List.of(cell));
            writePage(rootpage, rootpage.getPageNumber());
        } catch (DavisBaseException e) {
            if (!rootpage.hasEnoughSpaceForCells(List.of(cell))) {
                splitpage(rootpage, Cell cell);
                // using the returned file make page and then write the data;

            }
        }
    }
    private void splitpage(Page current_page, Cell cell) throws IOException {

        if (PageTypes.LEAF == current_page.getPageType()) {
            splitIndexRecordsBetweenPages(current_page, cell);
            int new_page = current_page.getPageNumber()+1;
        } else if (PageTypes.INTERIOR == current_page.getPageType()) {
            splitIndexRecordsBetweenPages(current_page,cell);
            int new_page = current_page.getPageNumber()+1;
        } else {
            throw new DavisBaseException("Unknown page type to continue with splitting");
        }
    }

    public void splitIndexRecordsBetweenPages(Page currentpage, com.neodymium.davisbase.models.Cell IndexValues) {

        int new_pageNo = currentpage.getPageNumber() + 1;
        Page newLeafPage = new Page(PAGE_SIZE, (short) new_pageNo, PageTypes.LEAF_INDEX,
                currentpage.getRootPage(), currentpage.getParentPage(), (short) 0xFFFF);

        try{
            List<com.neodymium.davisbase.models.Cell> left_node = new ArrayList<>(currentpage.getCells());
            left_node.add(IndexValues);
            left_node.sort(Comparator.comparing(c -> (Comparable)c.cellPayload().getkey()));
            int mid = left_node.size()/2;
            currentpage.setCells(left_node.subList(0,mid));
            newLeafPage.setCells(left_node.subList(mid+1,left_node.size()));

            com.neodymium.davisbase.models.Cell parentCell = newLeafPage.getCells().get(mid);
            handleParentPromotion(currentpage, newLeafPage, parentCell);
            writePage(currentpage, currentpage.getPageNumber());
            writePage(newLeafPage, new_pageNo);
        }
        catch(IOException e)
        {
            throw new DavisBaseException("Insert into Index File failed. Error while splitting index pages");
        }
    }


    private  void search(Page currentpage, Cell key_value, String searchvalue){
        int new_pageNo = currentpage.getPageNumber() + 1;
        Page newLeafPage = new Page(PAGE_SIZE, (short) new_pageNo, PageTypes.LEAF_INDEX,
                currentpage.getRootPage(), currentpage.getParentPage(), (short) 0xFFFF);


        List<Cell> searchList = new ArrayList<>();
        for(Cell cell: List.of(key_value)){
            searchList.add(cell.cellPayload().getKey();
            }
        if (searchList.contains(searchvalue)){ //sort
            writePage(currentpage, currentpage.getPageNumber());
            }
        else {
            searchList = Arrays.sort(searchList);
            int mid = searchList.size()/2;
            // where do I get the cell content values
            search(newLeafPage, leaf_cell_content, searchvalue);

        }
    }

    public short findChildPage(Object key) {
        List<Cell> sortedCells = new ArrayList<>(cells);
        sortedCells.sort(Comparator.comparing(cell -> cell.cellPayload().getKey()).reversed());

        return sortedCells.stream()
                .filter(cell -> ((Comparable) cell.cellPayload().getKey()).compareTo(key) < 0)
                .findFirst()
                .map(cell -> cell.cellHeader().leftChildPage())
                .orElse(siblingPage);
    }

    public void update(Cell cell) throws IOException {
        Optional<Page> pageOptional = searchPage(cell.cellPayload().getKey());
        if (pageOptional.isEmpty()) {
            throw new DavisBaseException("Row ID not found for update");
        }
        Page page = pageOptional.get();
        page.update(List.of(cell));
        writePage(page, page.getPageNumber());
    }


}




    private void writePage(Page page, int pageId) throws IOException {
        indexFile.seek((long) pageId * PAGE_SIZE);
        indexFile.write(page.serialize());
    }

    private Page loadPage(indexFile,short pageNo,int pageSize) {
        byte[] pageData = new byte[pageSize];
        indexFile.seek((long) pageNo * pageSize);
        indexFile.readFully(pageData);
        return new Page.deserialize(pageData,pageNo);
    }

    private Page getRootPage() throws IOException {
        short pageNo = (short) (indexFile.length() / pageSize);
        return loadPage(indexFile,pageNo,pageSize);
    }

    private void writePage(Page page, int pageId) throws IOException {
        indexFile.seek((long) pageId * PAGE_SIZE);
        indexFile.write(page.serialize());
    }







    //    // properties of a node
//    private final leftchild node;
//    private final rightchild node;
//    private final pagenumber;
//
//    // page/node type; can be imported
//    pubic boolean isInteriorNode;
//
//    // properties of cell ;
//    public Object IndexKeys;
//    Public List<Object> rowids;

}
