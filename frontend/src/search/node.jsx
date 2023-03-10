import React, { useCallback, useMemo, useContext } from "react";
import classNames from "classnames";
import { Badge } from "react-bootstrap";
import { NodeDetailsContext } from "../node-details/context";

const Node = ({ node, handleClickGraphBuilder }) => {
    const { nodeKey, name, description, tags } = node;
    const { openNodeDetails } = useContext(NodeDetailsContext);
    const graphBuilderButton = useMemo(() => {
        return (
            <button
                className={"btn btn-sm btn-light"}
                data-elem-name="graph-builder-button"
                onClick={() => {
                    handleClickGraphBuilder(nodeKey);
                }}
                data-toggle="tooltip"
                data-placement="top"
                title={"Add node to the Graph Builder"}
            >
                Add
            </button>
        );
    }, [nodeKey]);
    const handleNodeClick = useCallback(
        (e) => {
            // A click on the graph builder button will also activate the node click since the button is inside the div
            // this is registered too, we need to ensure we skip the click event in this situation.
            if (e.target.nodeName === "BUTTON") {
                return;
            }
            openNodeDetails(node);
        },
        [node, openNodeDetails]
    );

    return (
        <div
            className={classNames("list-group-item", "list-group-item-action")}
            onClick={handleNodeClick}
        >
            <div className="d-flex justify-content-between">
                <label
                    className="form-check-label text-truncate"
                    htmlFor={`nodeCheckbox-${name}`}
                >
                    <span className="fw-bold">{name}</span>
                </label>
                {graphBuilderButton}
            </div>
            {tags.map(({ id, text }) => (
                <Badge key={id} bg="light" text="dark" className="me-2 mt-2">
                    {text}
                </Badge>
            ))}
            <p>{description}</p>
        </div>
    );
};

export default Node;
