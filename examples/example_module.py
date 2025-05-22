from pathlib import Path
from typing import Any, cast

model = None


def segment(
    inputImage: Path | str,
    segmentation: Path | str | None = None,
    modelType="cyto",
    useGPU=False,
    channels=[0, 0],
    autoDiameter=True,
    diameter=30,
) -> Any:
    global model

    inputImage = Path(inputImage)
    if not inputImage.exists():
        raise Exception(f"Error: input image {inputImage} does not exist.")

    print(f"[[1/4]] Load libraries and model {modelType}")
    print("Loading libraries...")
    import cellpose.models  # type: ignore
    import cellpose.io  # type: ignore
    import numpy as np  # type: ignore

    if model is None or model.cp.model_type != modelType:
        print("Loading model...")
        model = cellpose.models.Cellpose(gpu=True if useGPU == "True" else False, model_type=modelType)

    print(f"[[2/4]] Load image {inputImage}")
    image = cast(np.ndarray, cellpose.io.imread(str(inputImage)))

    print("[[3/4]] Compute segmentation", image.shape)
    try:
        kwargs: Any = dict(diameter=int(diameter)) if autoDiameter else {}
        masks, flows, styles, diams = model.eval(image, channels=channels, **kwargs)
    except Exception as e:
        print(e)
        raise e
    print("segmentation finished.")

    # If segmentation is None: return all results
    if segmentation is None:
        return masks, flows, styles, diams

    segmentation = Path(segmentation)
    print(f"[[4/4]] Save segmentation {segmentation}")
    # save results as png
    cellpose.io.save_masks(image, masks, flows, str(inputImage), png=True)
    output_mask = inputImage.parent / f"{inputImage.stem}_cp_masks.png"
    if output_mask.exists():
        if segmentation.exists():
            segmentation.unlink()
        (output_mask).rename(segmentation)
        print(f"Saved out: {segmentation}")
    else:
        print("Segmentation was not generated because no masks were found.")

    return diams
