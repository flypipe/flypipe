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
import {
    getPredecessorNodesAndEdgesFromNode,
    moveToNode,
    refreshNodePositions,
} from "../util";
import { useReactFlow, MarkerType } from "reactflow";
import { NotificationContext } from "../../context";
import uuid from 'react-uuid';

// Maximum number of search entries per page
const SEARCH_PAGE_SIZE = 8;
// Maximum number of pages to show at the bottom of the screen
const SEARCH_PAGE_GROUP_SIZE = 5;

const NodeResultsList = ({ nodes, allNodes }) => {
    const { setNewMessage } = useContext(NotificationContext);
    const graph = useReactFlow();
    const [currentNodes, setCurrentNodes] = useState([]);
    const [graphBuilderNodes, setGraphBuilderNodes] = useState(new Set());
    // const {addNodesAndEdges, removeNodesAndEdges} = useStore(({addNodesAndEdges, removeNodesAndEdges}) => ({addNodesAndEdges, removeNodesAndEdges}));

    // Use need to use a ref to access the updated graphBuilderNodes
    // (https://stackoverflow.com/questions/55265255/react-usestate-hook-event-handler-using-initial-state)
    // Still unsure why exactly the state doesn't update in callbacks here in particular though as it
    //  normally seems to work fine.
    const graphBuilderNodesRef = useRef(graphBuilderNodes);
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
        const graphBuilderNodes = graphBuilderNodesRef.current;
        const { name } = nodes.find((node) => node.nodeKey === nodeKey);

        const [predecessorNodes, predecessorEdges] =
            getPredecessorNodesAndEdgesFromNode(allNodes, nodeKey);
        if (!graphBuilderNodes.has(nodeKey)) {
            setGraphBuilderNodes((prevGraphBuilderNodes) => {
                const newSet = new Set([...prevGraphBuilderNodes, nodeKey]);
                graphBuilderNodesRef.current = newSet;
                return newSet;
            });
        }
        // Add the node and any predecessor nodes/edges to the graph that aren't already there.
        const currentNodes = new Set(graph.getNodes().map(({ id }) => id));
        const currentEdges = new Set(graph.getEdges().map(({ id }) => id));
        const newNodes = predecessorNodes.filter(
            ({ id }) => !currentNodes.has(id)
        );
        const newEdges = predecessorEdges.filter(
            ({ id }) => !currentEdges.has(id)
        );
        
        newEdges.forEach((e,i) => {
            newEdges[i]['markerEnd'] = {
                type: MarkerType.ArrowClosed,
                width: 15,
                height: 15,
            }
        })
        
        graph.addNodes(newNodes);
        graph.addEdges(newEdges);
        refreshNodePositions(graph);
        setNewMessage({
            msgId: uuid(),
            message: `Added node ${name} and ancestor nodes/edges to the graph`
        });
        moveToNode(graph, nodeKey);
    };

    return (
        <div className="list-group list-group-flush overflow-auto list-group-flypipe pb-2">
            {currentNodes.map(
                ({ nodeKey, name, importCmd, description }, i) => (
                    <Node
                        key={`node-list-item-${i}`}
                        nodeKey={nodeKey}
                        name={name}
                        importCmd={importCmd}
                        description={description}
                        isInGraphBuilder={graphBuilderNodes.has(nodeKey)}
                        handleClickGraphBuilder={handleClickGraphBuilder}
                    />
                )
            )}
            <Pagination
                maxPage={maxPage}
                pageGroupSize={SEARCH_PAGE_GROUP_SIZE}
                handleClickPage={handleChangePage}
            />
        </div>
    );
};

export default NodeResultsList;
