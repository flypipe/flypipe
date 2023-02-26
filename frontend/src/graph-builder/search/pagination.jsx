import React, { useState, useMemo, useCallback, useEffect } from "react";
import classNames from "classnames";

const Pagination = ({ maxPage, pageGroupSize, handleClickPage }) => {
    const initialPageGroup = useMemo(() => {
        let initialPageGroup = [];
        for (let i = 1; i < Math.min(pageGroupSize, maxPage) + 1; i++) {
            initialPageGroup.push(i);
        }
        return initialPageGroup;
    }, [pageGroupSize, maxPage]);

    const [currentPage, setCurrentPage] = useState(1);

    // Currently visible slice of pages, for example pages 1, 2, 3, 4, 5
    const [pageGroup, setPageGroup] = useState([]);
    useEffect(() => {
        setPageGroup(initialPageGroup);
    }, [initialPageGroup]);

    const onPageClick = useCallback(
        (targetPage) => {
            setCurrentPage(targetPage);
            handleClickPage(targetPage);
        },
        [handleClickPage]
    );

    const onLeftMovePagesClick = useCallback(
        (e) => {
            e.preventDefault();
            let newPageGroup = [];
            const firstVisiblePage = pageGroup[0];
            if (firstVisiblePage - pageGroupSize < 1) {
                // There are not enough pages to fill up the previous slice of pages, let's move just enough to reach the
                // start of the page list.
                for (let i = 1; i < pageGroupSize + 1; i++) {
                    newPageGroup.push(i);
                }
            } else {
                for (
                    let i = firstVisiblePage - pageGroupSize;
                    i < firstVisiblePage;
                    i++
                ) {
                    newPageGroup.push(i);
                }
            }
            setPageGroup(newPageGroup);
        },
        [pageGroup, maxPage, pageGroupSize]
    );

    const onRightMovePagesClick = useCallback(
        (e) => {
            e.preventDefault();
            let newPageGroup = [];
            const lastVisiblePage = pageGroup[pageGroup.length - 1];
            if (lastVisiblePage + pageGroupSize > maxPage) {
                // There are not enough remaining pages to fill up the next slice of pages, let's advance just enough to
                // reach the end of the page list.
                for (
                    let i = maxPage - pageGroupSize + 1;
                    i < maxPage + 1;
                    i++
                ) {
                    newPageGroup.push(i);
                }
            } else {
                for (
                    let i = lastVisiblePage + 1;
                    i < lastVisiblePage + pageGroupSize + 1;
                    i++
                ) {
                    newPageGroup.push(i);
                }
            }
            setPageGroup(newPageGroup);
        },
        [pageGroup, maxPage, pageGroupSize]
    );

    if (pageGroup.length == 0) {
        return <div className="ms-3 fw-bold fs-6">No results found</div>;
    }
    return (
        <nav aria-label="Node search result pages">
            <ul className="pagination d-flex justify-content-center">
                <li className="page-item">
                    <a
                        className={classNames("page-link flypipe", {
                            disabled: pageGroup[0] === 1,
                        })}
                        href="#"
                        onClick={onLeftMovePagesClick}
                    >
                        {"<<"}
                    </a>
                </li>
                {pageGroup.map((pageNumber) => (
                    <li key={`page-${pageNumber}`} className="page-item">
                        <a
                            className={classNames(
                                "page-link flypipe text-secondary",
                                {
                                    "fw-bold": pageNumber === currentPage,
                                }
                            )}
                            href="#"
                            onClick={(e) => {
                                e.preventDefault();
                                onPageClick(pageNumber);
                            }}
                        >
                            {pageNumber}
                        </a>
                    </li>
                ))}

                <li className="page-item">
                    <a
                        className={classNames("page-link flypipe", {
                            disabled:
                                pageGroup[pageGroup.length - 1] === maxPage,
                        })}
                        href="#"
                        onClick={onRightMovePagesClick}
                    >
                        {">>"}
                    </a>
                </li>
            </ul>
        </nav>
    );
};

export default Pagination;
