from wetlands.ndarray import NDArray
import example_module


def segment(image: NDArray, segmentation: NDArray):
    masks, _, _, _ = example_module.segment_image(image.array)
    segmentation.array[:] = masks[:]
    # Close shared memories so that they can be freed on the other side
    image.close()
    segmentation.close()
