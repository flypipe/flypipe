import React, {
    useCallback,
    useMemo,
    useRef,
    useEffect,
    useContext,
} from "react";
import { useReactFlow, Handle, Position } from "reactflow";
import Badge from "react-bootstrap/Badge";
import {
    refreshNodePositions,
    NODE_WIDTH,
    NODE_HEIGHT,
    getEdgeDef,
    addOrReplaceEdge,
    getNodeTypeColorClass,
} from "../util";
import classNames from "classnames";
import textFit from "textfit";
import { GrNew } from "react-icons/gr";
import { BsDatabase, BsDatabaseSlash } from "react-icons/bs";
import { LuFileInput } from "react-icons/lu";

import { NotificationContext } from "../notifications/context";
import { GraphContext } from "./graph-context";

const BaseNode = ({ data, isNewNode }) => {
    const graph = useReactFlow();
    const {
        nodeType,
        label,
        isActive,
        hasCache,
        cacheIsDisabled,
        hasProvidedInput,
    } = data;
    const { addNotification } = useContext(NotificationContext);
    const { setCurrentGraphObject } = useContext(GraphContext);

    const handleConnect = useCallback(
        ({ source, target }) => {
            const sourceLabel = graph.getNode(source).data.label;
            const targetLabel = graph.getNode(target).data.label;
            const edge = getEdgeDef(graph, source, target);
            addOrReplaceEdge(graph, edge);
            refreshNodePositions(graph);
            addNotification(
                `Dependency on ${sourceLabel} added to ${targetLabel}`
            );
            setCurrentGraphObject({
                object: edge,
                type: "edge",
            });
        },
        [graph, addNotification, setCurrentGraphObject]
    );

    const nodeClass = `node-${getNodeTypeColorClass(nodeType)}`;
    const klass = useMemo(() =>
        classNames(
            "d-flex",
            "container",
            isNewNode ? "justify-content-between" : "justify-content-center",
            "px-4",
            "py-2",
            "rounded",
            isActive ? "" : "node-dashed",
            nodeClass
        )
    );

    const nodeTextRef = useRef(null);
    useEffect(() => {
        if (nodeTextRef.current) {
            textFit(nodeTextRef.current, { alignHoriz: true, alignVert: true });
        }
    }, [nodeTextRef.current, label]);

    // Within the node we must divide the space between the name and the badge
    const [nameWidth, badgeWidth] = useMemo(() => {
        if (isNewNode) {
            return ["col-12", ""];
        } else {
            return ["col-12", ""];
        }
    }, [isNewNode]);

    const onCheckValidConnection = useCallback(
        ({ target }) => {
            return graph.getNode(target).data.isNew;
        },
        [graph]
    );

    const margin = hasProvidedInput && hasCache ? "me-5" : "";

    return (
        <>
            <Handle
                type="target"
                position={Position.Left}
                id="target-handle"
                onConnect={handleConnect}
                isValidConnection={onCheckValidConnection}
            />
            <div
                className={klass}
                style={{
                    width: NODE_WIDTH,
                    height: NODE_HEIGHT,
                    cursor: "pointer",
                }}
            >
                <p
                    className={`${nameWidth} mb-0 me-2`}
                    style={{ textAlign: "center" }}
                    ref={nodeTextRef}
                >
                    {label}
                </p>
                {isNewNode && (
                    <Badge
                        pill
                        bg="warning"
                        className={`${badgeWidth} align-self-start fs-6 position-absolute node-badge`}
                        title="New node"
                        size="md"
                    >
                        <GrNew className="fs-4" />
                    </Badge>
                )}
                <div className="d-flex flex-row-reverse">
                    {hasProvidedInput && (
                        <Badge
                            pill
                            bg="light"
                            className={`${badgeWidth} align-self-end fs-6 position-absolute node-badge ${margin}`}
                            title="Input provided"
                            size="xs"
                        >
                            <LuFileInput className="fs-4" color="black" />
                        </Badge>
                    )}
                    {hasCache && cacheIsDisabled && (
                        <Badge
                            pill
                            bg="light"
                            className={`${badgeWidth} align-self-end fs-6 position-absolute node-badge`}
                            title="Cache (disabled)"
                            size="md"
                        >
                            <BsDatabaseSlash className="fs-4" color="red" />
                        </Badge>
                    )}
                    {hasCache && !cacheIsDisabled && (
                        <Badge
                            pill
                            bg="light"
                            className={`${badgeWidth} align-self-start fs-6 position-absolute node-badge`}
                            title="Cache"
                            size="md"
                        >
                            <BsDatabase className="fs-4" color="black" />
                        </Badge>
                    )}
                </div>
            </div>
            <Handle
                type="source"
                position={Position.Right}
                id="source-handle"
                onConnect={handleConnect}
                isValidConnection={onCheckValidConnection}
            />
        </>
    );
};

const ExistingNode = (props) => <BaseNode isNewNode={false} {...props} />;
const NewNode = (props) => <BaseNode isNewNode={true} {...props} />;

export { ExistingNode, NewNode };
