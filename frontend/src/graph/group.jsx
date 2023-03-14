import React from "react";

const GroupNode = ({ data }) => {
    const { label } = data;
    return (
        <>
            <Handle
                type="target"
                position={Position.Left}
                id="target-handle"
                isValidConnection={false}
            />
            <div
                className={klass}
                // style={{
                //     width,
                //     height,
                // }}
            >
                <p
                    className={`${nameWidth} mb-0 me-2`}
                    style={{ textAlign: "center" }}
                    ref={nodeTextRef}
                >
                    {label}
                </p>
            </div>
            <Handle
                type="source"
                position={Position.Right}
                id="source-handle"
                isValidConnection={false}
            />
        </>
    );
};

export default GroupNode;
