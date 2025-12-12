from pathlib import Path
from typing import TYPE_CHECKING, Any

model = None

if TYPE_CHECKING:
    import numpy


def segment_image(
    image: numpy.ndarray,
    model_type="cyto",
    use_gpu=False,
    channels=[0, 0],
    auto_diameter=True,
    diameter=30,
) -> Any:
    global model

    print("Loading libraries...")
    import cellpose.models  # type: ignore

    if model is None or model.cp.model_type != model_type:
        print(f"Loading model {model_type}...")
        model = cellpose.models.Cellpose(gpu=True if use_gpu == "True" else False, model_type=model_type)

    print("Compute segmentation...")
    try:
        kwargs: Any = dict(diameter=int(diameter)) if auto_diameter else {}
        masks, flows, styles, diams = model.eval(image, channels=channels, **kwargs)
    except Exception as e:
        print(e)
        raise e
    print("Segmentation finished.")
    return masks, flows, styles, diams


def segment(
    input_image: Path | str,
    model_type="cyto",
    use_gpu=False,
    channels=[0, 0],
    auto_diameter=True,
    diameter=30,
    return_segmentation=False,  # return segmentation or save it in a file
) -> Any:
    global model

    import cellpose.io  # type: ignore

    input_image = Path(input_image)
    image = cellpose.io.imread(input_image)

    masks, flows, styles, diams = segment_image(image, model_type, use_gpu, channels, auto_diameter, diameter)

    # If return_segmentation: return masks, otherwise save them and return diams
    if return_segmentation:
        return masks

    # save results as png
    cellpose.io.save_masks(image, masks, flows, str(input_image), png=True)

    return diams
