# Semi Deduped Storage

Semi Deduped storage is already implemented, but not yet used for any existing [vdisk][vdisk] types.

The code for this storage type can be found in [/nbdserver/ardb/semideduped.go](/nbdserver/ardb/semideduped.go).

You can read the [storage docs](/docs/nbd/storage/storage.md) for more information about other available storage types.

## Storage

Semi Deduped storage is a hybrid storage, combining the [non-deduped][nondeduped]- and [deduped][deduped] storage. The [non-deduped][nondeduped] [blocks][block] are writiable and filled by the user, while the [deduped][deduped] [blocks][block] is read-only and filled by the _required_ [template][template] [storage (1)][storage] cluster. The Semi Deduped storage is the only storage type which doesn't work without a defined [template][template] [storage (1)][storage] cluster.

![Semi Deduped Storage](/docs/assets/nbd_semideduped_storage.png)

A bitmap is used to define whether a [block][block] has to be read using the [deduped][deduped]- or [non-deduped][nondeduped] storage. Each block [index (2)][index] corresponds directly to the bit position in the bitmap. Thus block [index (2)][index] `5`, is controlled by bit `5`, for example.

If a [block][block]'s bit is set, the content is attempted to be read from writable [non-deduped][nondeduped] storage. If this [block][block]'s bit is not set or if the content couldn't be found in the [non-deduped][nondeduped] storage, the [block][block] is attempted to be read from the read-only [deduped][deduped] storage. If this is the case, the [block][block] could also come from the [template][template] [storage (1)][storage] cluster instead of the primary [storage (1)][storage] cluster. This is only the case of course if the [vdisk][vdisk] has [template][template] support.

This bitmap gets compressed ([gzip][gzip]) and stored in the [metadata (2)][metadata] [storage (1)][storage] cluster when the storage gets flushed. The bitmap is kept in memory and is only read (and decompressed) from the [metadata (2)][metadata] [storage (1)][storage] when the storage gets created (right at the start).

Because only `1 bit` per [block][block] is required the size in memory for this bitmap stays quite small. For a [vdisk][vdisk] with a size of `200 GiB`, and a [block][block] size of `4 KiB`, the bitmap's maximum size would still only be `6400 KiB`. When compressed and stored in the [metadata (2)][metadata] [storage (1)][storage] cluster this size gets even smaller, as the bitmap compress ratio is really high. Keeping the bitmap in memory also keeps the CPU cost low.

## Possible Failures

Any read/write operation will fail if the underlying [deduped][deduped]- or [non-deduped][nondeduped] storage fails for any reason.


[vdisk]: /docs/glossary.md#vdisk
[template]: /docs/glossary.md#template
[block]: /docs/glossary.md#block
[index]: /docs/glossary.md#index
[storage]: /docs/glossary.md#storage
[data]: /docs/glossary.md#data
[metadata]: /docs/glossary.md#metadata

[deduped]: /docs/nbd/storage/deduped.md
[nondeduped]: /docs/nbd/storage/nondeduped.md

[gzip]: https://golang.org/pkg/compress/gzip/
