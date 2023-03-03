import React, {
    useState,
    useMemo,
    useCallback,
    useEffect,
    useRef,
    useContext,
} from "react";
import Node from "./node";
import Pagination from "./pagination";
import { addNodeAndPredecessors, moveToNode } from "../util";
import { useReactFlow, MarkerType } from "reactflow";
import { NotificationContext } from "../notifications/context";
import uuid from "react-uuid";

// Maximum number of search entries per page
const SEARCH_PAGE_SIZE = 8;
// Maximum number of pages to show at the bottom of the screen
const SEARCH_PAGE_GROUP_SIZE = 5;

const NodeResultsList = ({ nodes, allNodes }) => {
    const { setNewMessage } = useContext(NotificationContext);
    const graph = useReactFlow();
    const [currentNodes, setCurrentNodes] = useState([]);

    // nodes can change via the node search panel, we must reset the nodes if so
    useEffect(() => {
        setCurrentNodes(nodes.slice(0, SEARCH_PAGE_SIZE));
    }, [nodes]);

    const maxPage = useMemo(
        () => Math.ceil(nodes.length / SEARCH_PAGE_SIZE),
        [nodes]
    );
    const nodesByPage = useMemo(() => {
        const result = {};
        for (let i = 1; i < maxPage + 1; i++) {
            result[i] = nodes.slice(
                (i - 1) * SEARCH_PAGE_SIZE,
                i * SEARCH_PAGE_SIZE
            );
        }
        return result;
    }, [nodes]);
    const handleChangePage = useCallback(
        (pageNumber) => {
            setCurrentNodes(nodesByPage[pageNumber]);
        },
        [nodesByPage]
    );

    const handleClickGraphBuilder = (nodeKey) => {
        const { name } = nodes.find((node) => node.nodeKey === nodeKey);

        addNodeAndPredecessors(graph, allNodes, nodeKey);

        setNewMessage({
            msgId: uuid(),
            message: `Added node ${name} and ancestor nodes/edges to the graph`,
        });
        moveToNode(graph, nodeKey);
    };

    return (
        <div className="list-group list-group-flush overflow-auto list-group-flypipe">
            {currentNodes.map((node, i) => (
                <Node
                    key={`node-list-item-${i}`}
                    node={node}
                    handleClickGraphBuilder={handleClickGraphBuilder}
                />
            ))}
            <Pagination
                maxPage={maxPage}
                pageGroupSize={SEARCH_PAGE_GROUP_SIZE}
                handleClickPage={handleChangePage}
            />
        </div>
    );
};

export default NodeResultsList;
